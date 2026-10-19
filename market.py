import asyncio,json,logging,math,random,time
from collections import deque,Counter
from dataclasses import dataclass
from contextlib import suppress
import websockets
from assets import SYMBOL_NAMES
from protocol import Decoder,ticks,unpack
from clock import Clock
log=logging.getLogger('radar.market')

@dataclass(frozen=True)
class Tick:
    timestamp:float
    price:float
    received:float

class Store:
    def __init__(self,capacity=30000,retention=900):
        self.data={s:deque(maxlen=capacity) for s in SYMBOL_NAMES};self.retention=retention
        self.accepted=0;self.rejected=0;self.reasons=Counter();self.last_reason='NONE'
    def clear(self):
        for q in self.data.values():q.clear()
    def reject(self,reason):
        self.last_reason=reason;self.reasons[reason]+=1;self.rejected+=1;return False
    def add(self,symbol,stamp,price,wall=None,mono=None):
        if not isinstance(symbol,str) or symbol not in self.data:return self.reject('unknown_symbol')
        wall=time.time() if wall is None else wall;mono=time.monotonic() if mono is None else mono
        try:
            stamp=float(stamp);price=float(price)
            if stamp>1e11:stamp/=1000
            if not math.isfinite(stamp) or not math.isfinite(price) or price<=0:return self.reject('invalid_number')
            if stamp>wall+3:return self.reject('future_timestamp')
            if wall-stamp>15:return self.reject('old_timestamp')
            q=self.data[symbol]
            if q and stamp<=q[-1].timestamp:return self.reject('duplicate_or_out_of_order')
            q.append(Tick(stamp,price,mono))
            while q and q[0].timestamp<stamp-self.retention:q.popleft()
            self.accepted+=1;self.last_reason='NONE';return True
        except (TypeError,ValueError,OverflowError):return self.reject('invalid_number')
    def snapshot(self):return {s:tuple(q) for s,q in self.data.items()}
    def fresh_count(self,limit=5):
        mono=time.monotonic();wall=time.time()
        return sum(bool(q) and -3<=wall-q[-1].timestamp<=limit and mono-q[-1].received<=limit for q in self.data.values())

class Feed:
    def __init__(self,cfg,store):
        self.cfg=cfg;self.store=store;self.connected=False;self.last_error='NOT_CONNECTED'
        self.reconnects=0;self.ws=None;self.last_tick_at=0;self.subscribed=0
        self.stats=Counter();self.decoder=Decoder();self.clock=Clock(getattr(cfg,'timestamp_offset','auto'))
        self.seen_symbols=set();self.first_tick_logged=False;self.resubscriptions=0
    def diagnostics(self):
        return dict(code=self.last_error,authenticated=self.connected,frames=self.stats['frames'],
          stream_events=self.stats['stream_events'],quote_rows=self.stats['quote_rows'],
          accepted=self.store.accepted,rejected=self.store.rejected,reject_reasons=dict(self.store.reasons),
          decoder_errors=self.decoder.stats['decode_errors'],offsets={str(k):v for k,v in self.clock.offsets.items()},
          distinct_symbols=len(self.seen_symbols),subscribed=self.subscribed)
    def no_data_code(self):
        if not self.stats['stream_events']:return 'NO_STREAM_EVENTS'
        if not self.stats['quote_rows']:return 'STREAM_FORMAT_UNSUPPORTED'
        if self.store.reasons['clock_out_of_range']:return 'QUOTE_CLOCK_UNSUPPORTED'
        if self.store.rejected:return 'QUOTES_REJECTED'
        return 'STREAM_STALE'
    def process(self,event):
        if not isinstance(event,list) or len(event)<2:return
        name,payload=event[:2];payload=unpack(payload)
        if name in ('failauth','errorauth','authError'):raise PermissionError('AUTH_REJECTED')
        if name=='updateStream':
            self.stats['stream_events']+=1
            for s,t,p in ticks(payload):
                self.stats['quote_rows']+=1
                if not isinstance(s,str) or s not in self.store.data:self.store.reject('unknown_symbol');continue
                self.seen_symbols.add(s);wall=time.time();mono=time.monotonic()
                stamp=self.clock.convert(s,t,wall,mono)
                if stamp is None:self.store.reject(self.clock.last_reason);continue
                if self.store.add(s,stamp,p,wall,mono):
                    self.last_tick_at=mono
                    if not self.first_tick_logged:
                        log.info('First live quote accepted; freshness checks passed')
                        self.first_tick_logged=True
    async def emit(self,name,payload=None):
        # Deliberate whitelist: no trading commands can be emitted.
        if name not in ('subscribeSymbol','changeSymbol','indicator/load','favorite/load','price-alert/load','ps'):
            raise ValueError('READ_ONLY_EVENT_BLOCKED')
        await self.ws.send('42'+json.dumps([name] if payload is None else [name,payload],separators=(',',':')))
    async def subscribe(self):
        browser=getattr(self.cfg,'subscription_mode','browser')=='browser'
        if browser:
            for name in ('indicator/load','favorite/load','price-alert/load'):await self.emit(name)
        for symbol in SYMBOL_NAMES:
            await self.emit('subscribeSymbol',symbol)
            if browser:await self.emit('changeSymbol',{'asset':symbol,'period':30})
            self.subscribed+=1
            await asyncio.sleep(.08)
    async def _session(self):
        self.store.clear();self.last_tick_at=0;self.subscribed=0;self.stats.clear();self.store.reasons.clear()
        self.store.accepted=0;self.store.rejected=0;self.clock=Clock(getattr(self.cfg,'timestamp_offset','auto'))
        self.decoder=Decoder();self.seen_symbols.clear();self.first_tick_logged=False;self.resubscriptions=0
        self.last_error='CONNECTING';authorized=False
        headers={'Origin':'https://pocketoption.com','User-Agent':'Mozilla/5.0'}
        async with websockets.connect(self.cfg.url,additional_headers=headers,ping_interval=None,
                 open_timeout=15,close_timeout=3,max_size=2**22) as ws:
            self.ws=ws
            async with asyncio.timeout(25):
                greeting=await ws.recv()
                if not isinstance(greeting,str) or not greeting.startswith('0'):raise RuntimeError('BAD_HANDSHAKE')
                await ws.send('40')
                while True:
                    f=await ws.recv()
                    if isinstance(f,str) and f.startswith('2'):await ws.send('3'+f[1:])
                    elif isinstance(f,str) and f.startswith('40'):break
                await ws.send(self.cfg.auth)
                while not authorized:
                    frame=await ws.recv();self.stats['frames']+=1
                    if isinstance(frame,str) and frame.startswith('2'):await ws.send('3'+frame[1:]);continue
                    for ev in self.decoder.feed(frame):
                        self.process(ev)
                        if isinstance(ev,list) and len(ev)>1:
                            data=unpack(ev[1])
                            authorized=authorized or ev[0]=='successauth' or (ev[0] in ('successupdateBalance','updateBalance') and isinstance(data,dict) and 'balance' in data)
            self.connected=True;self.last_error='WAITING_QUOTES'
            log.info('Feed authenticated; preparing read-only quote subscriptions')
            async def upkeep():
                await self.subscribe();last_sub=time.monotonic();last_ps=0;last_log=0
                while True:
                    now=time.monotonic()
                    if now-last_ps>=20:await self.emit('ps');last_ps=now
                    if now-last_log>=15:
                        log.info('Feed diagnostics: %s',json.dumps(self.diagnostics(),sort_keys=True));last_log=now
                    # One repair attempt without erasing a warm buffer.
                    if not self.last_tick_at and now-last_sub>=12 and self.resubscriptions==0:
                        self.resubscriptions+=1;await self.subscribe()
                    await asyncio.sleep(1)
            task=asyncio.create_task(upkeep());started=time.monotonic()
            try:
                while True:
                    if task.done():task.result()
                    try:frame=await asyncio.wait_for(ws.recv(),timeout=2)
                    except asyncio.TimeoutError:
                        if time.monotonic()-(self.last_tick_at or started)>self.cfg.stream_timeout:
                            self.last_error=self.no_data_code();raise TimeoutError()
                        continue
                    self.stats['frames']+=1
                    if isinstance(frame,str) and frame.startswith('2'):await ws.send('3'+frame[1:])
                    elif frame=='1':raise ConnectionError('SERVER_CLOSE')
                    else:
                        for ev in self.decoder.feed(frame):self.process(ev)
                    if self.last_tick_at:self.last_error='LIVE_QUOTES'
                    if time.monotonic()-(self.last_tick_at or started)>self.cfg.stream_timeout:
                        self.last_error=self.no_data_code();raise TimeoutError()
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):await task
    async def run(self):
        delay=1
        while True:
            started=time.monotonic()
            try:await self._session()
            except asyncio.CancelledError:raise
            except Exception as exc:
                if isinstance(exc,PermissionError):self.last_error='AUTH_REJECTED'
                elif self.last_error not in ('NO_STREAM_EVENTS','STREAM_FORMAT_UNSUPPORTED','QUOTE_CLOCK_UNSUPPORTED','QUOTES_REJECTED','STREAM_STALE'):
                    self.last_error='CONNECT_TIMEOUT' if isinstance(exc,TimeoutError) else 'WS_DISCONNECTED'
                log.warning('Feed unavailable code=%s type=%s diagnostics=%s',self.last_error,type(exc).__name__,json.dumps(self.diagnostics(),sort_keys=True))
            finally:self.connected=False;self.ws=None;self.store.clear()
            self.reconnects+=1
            if time.monotonic()-started>60:delay=1
            await asyncio.sleep(delay+random.uniform(0,.5));delay=min(delay*2,30)
