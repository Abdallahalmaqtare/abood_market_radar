import asyncio,csv,io,json,logging,signal,time
from contextlib import suppress
from aiogram import Bot,Dispatcher,BaseMiddleware,F
from aiogram.filters import CommandStart,Command
from aiogram.types import InlineKeyboardMarkup,InlineKeyboardButton,BufferedInputFile,CallbackQuery
from aiogram.exceptions import TelegramBadRequest
from aiohttp import web
from assets import SYMBOL_NAMES
from config import Config
from market import Store,Feed
from analysis import evaluate,PROFILES
from report import report
log=logging.getLogger('radar')

class AdminOnly(BaseMiddleware):
    def __init__(self,admin):self.admin=admin
    async def __call__(self,handler,event,data):
        user=getattr(event,'from_user',None)
        message=event.message if isinstance(event,CallbackQuery) else event
        private=getattr(getattr(message,'chat',None),'type',None)=='private'
        if not user or user.id!=self.admin or not private:
            if isinstance(event,CallbackQuery):
                with suppress(Exception):await event.answer()
            return None
        return await handler(event,data)

def keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
     [InlineKeyboardButton(text='⚡ فحص 5–30 ثانية',callback_data='scan:fast')],
     [InlineKeyboardButton(text='🕯 فحص 1–5 دقائق',callback_data='scan:slow')],
     [InlineKeyboardButton(text='📡 الحالة',callback_data='status'),InlineKeyboardButton(text='ℹ️ طريقة التقييم',callback_data='about')],
     [InlineKeyboardButton(text='📎 تفاصيل آخر فحص CSV',callback_data='export')]])

class Controller:
    def __init__(self,cfg,feed,store,bot):
        self.cfg=cfg;self.feed=feed;self.store=store;self.bot=bot
        self.scan_task=None;self.last_results=None;self.last_profile=None;self.last_scan=0
        self.started=time.monotonic()
    def status(self):
        covered=self.store.fresh_count(self.cfg.stale_seconds)
        return ('📡 <b>Abood Market Radar</b>\n'
          f'الاتصال: {"🟢 متصل" if self.feed.connected else "🔴 غير متصل"}\n'
          f'الأزواج ذات تكات حديثة: {covered}/{len(SYMBOL_NAMES)}\n'
          f'اشتراكات أُرسلت: {self.feed.subscribed} (ليست تأكيد قبول)\n'
          f'إعادة الاتصال: {self.feed.reconnects}\n'
          f'كود الحالة: {self.feed.last_error}\n'
          'تهيئة الأفق السريع: نحو 3 دقائق؛ الدقائق: نحو 10 دقائق بعد اتصال مستقر.\n'
          'المشروع للقراءة فقط ولا ينفذ صفقات.')
    async def scan(self,chat,profile):
        start=time.monotonic()
        try:
            msg=await self.bot.send_message(chat,'🔎 جارٍ فحص أزواج الفوركس OTC…\nيُرسل التقرير المتاح حتى لو كانت بعض البيانات ناقصة.')
            # Quotes accumulate concurrently. No per-pair network calls here.
            await asyncio.sleep(self.cfg.scan_seconds)
            snapshot=self.store.snapshot();now=time.time();mono=time.monotonic()
            results=[evaluate(s,points,profile,now,mono,self.cfg.stale_seconds) for s,points in snapshot.items()]
            if not self.feed.connected:
                for r in results:r.category='unknown';r.reasons=['البث غير متصل؛ لا اعتماد للبيانات']
            self.last_results=results;self.last_profile=profile
            pages=report(results,profile,self.feed.connected,time.monotonic()-start)
            # Edit the preparation message to reduce delivery work; Telegram delivery isn't guaranteed.
            await msg.edit_text(pages[0],reply_markup=keyboard() if len(pages)==1 else None)
            for i,page in enumerate(pages[1:]):
                await self.bot.send_message(chat,page,reply_markup=keyboard() if i==len(pages)-2 else None)
            log.info('Scan completed profile=%s assessed=%d/%d',profile,sum(r.category!='unknown' for r in results),len(results))
        except asyncio.CancelledError:raise
        except Exception as exc:
            log.warning('Scan failed type=%s',type(exc).__name__)
            with suppress(Exception):await self.bot.send_message(chat,'تعذر إيصال التقرير. أعد المحاولة؛ لم يتم اختراع أي نتيجة.',reply_markup=keyboard())
    async def callback(self,q):
        action=q.data or ''
        if action.startswith('scan:'):
            profile=action.split(':',1)[1]
            if profile not in PROFILES:await q.answer();return
            if self.scan_task and not self.scan_task.done():await q.answer('يوجد فحص جارٍ بالفعل');return
            if time.monotonic()-self.last_scan<5:await q.answer('انتظر بضع ثوانٍ بين الفحوص');return
            # Reserve the scan before any await, preventing double-tap races.
            self.last_scan=time.monotonic();self.scan_task=asyncio.create_task(self.scan(q.from_user.id,profile))
            with suppress(TelegramBadRequest):await q.answer('بدأ الفحص')
            return
        await q.answer()
        if action=='status':await q.message.answer(self.status(),reply_markup=keyboard())
        elif action=='about':await q.message.answer(
          'ℹ️ <b>كيف يعمل الرادار؟</b>\n'
          'يقارن التذبذب بخط أساس سابق لنفس الزوج، ويقيس القفزات والانعكاسات وانتظام المسار. '
          'يعيد أخذ عينات زمنية لتقليل تحيز كثرة التكات، ولا يملأ فجوات الأسعار بأسعار مختلقة.\n'
          'الأخضر: انتظام نسبي؛ الأصفر: مختلط؛ الأحمر: اضطراب؛ الأبيض: بيانات غير كافية.\n'
          'القواعد أولية وصفية وليست نموذجاً مُثبتاً للتنبؤ. لا توصيات CALL/PUT ولا نسب نجاح.\n'
          'لا نعرض قائمة الأزواج على أنها متاحة فعلياً إلا بوصول تكات حديثة؛ الرموز غير المدعومة تبقى دون تقييم.',reply_markup=keyboard())
        elif action=='export':
            if not self.last_results:await q.message.answer('نفّذ فحصاً أولاً.');return
            out=io.StringIO();writer=csv.writer(out)
            writer.writerow(['symbol','profile','classification','samples','age_seconds','volatility_ratio','spike_fraction','reversals','efficiency','coverage','rms_bps','reason'])
            for r in self.last_results:
                writer.writerow([r.symbol,self.last_profile,r.category,r.samples,round(r.age,2),*[r.metrics.get(k,'') for k in ('volatility_ratio','spike_fraction','reversals','efficiency','coverage','rms_bps')],' | '.join(r.reasons)])
            await q.message.answer_document(BufferedInputFile(out.getvalue().encode('utf-8-sig'),filename='radar_last_scan.csv'))

async def main():
    from aiogram.client.default import DefaultBotProperties
    cfg=Config.load();store=Store(cfg.tick_capacity,cfg.retention_seconds);feed=Feed(cfg,store)
    bot=Bot(cfg.token,default=DefaultBotProperties(parse_mode='HTML'))
    dp=Dispatcher();controller=Controller(cfg,feed,store,bot)
    dp.message.outer_middleware(AdminOnly(cfg.admin));dp.callback_query.outer_middleware(AdminOnly(cfg.admin))
    @dp.message(CommandStart())
    async def start(message):await message.answer('أهلاً بك في <b>Abood Market Radar</b> 📡\nاختر أفق الفحص. أسعار حقيقية فقط، بلا تنفيذ صفقات.',reply_markup=keyboard())
    @dp.message(Command('status'))
    async def status(message):await message.answer(controller.status(),reply_markup=keyboard())
    dp.callback_query.register(controller.callback)
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        with suppress(NotImplementedError):loop.add_signal_handler(sig,stop.set)
    app=web.Application()
    async def health(request):
        ready=feed.connected and store.fresh_count(cfg.stale_seconds)>0
        if request.path=='/ready':return web.json_response({'ready':ready},status=200 if ready else 503)
        # Public liveness does not expose admin ID, account data, assets or raw errors.
        return web.json_response({'service':'Abood Market Radar','alive':True})
    for path in ('/','/health','/ready'):app.router.add_get(path,health)
    runner=web.AppRunner(app);await runner.setup();await web.TCPSite(runner,'0.0.0.0',cfg.port).start()
    log.info('HTTP server ready; read-only radar starting')
    tasks=[]
    try:
        # Default is NOT to drop pending administrator messages.
        await bot.delete_webhook(drop_pending_updates=False)
        tasks=[asyncio.create_task(feed.run()),asyncio.create_task(dp.start_polling(bot,handle_signals=False,close_bot_session=False)),asyncio.create_task(stop.wait())]
        done,_=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if not task.cancelled():task.result()
    finally:
        if controller.scan_task:controller.scan_task.cancel()
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,*([controller.scan_task] if controller.scan_task else []),return_exceptions=True)
        await bot.session.close();await runner.cleanup()

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    # Library network errors can contain endpoints; suppress their exception text.
    logging.getLogger('aiogram').setLevel(logging.CRITICAL)
    try:asyncio.run(main())
    except KeyboardInterrupt:pass
    except Exception as exc:
        logging.getLogger('radar').error('Application stopped type=%s; check configuration and connectivity',type(exc).__name__)
        raise SystemExit(1) from None
