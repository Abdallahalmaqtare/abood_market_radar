from datetime import datetime,timedelta,timezone
from html import escape
from assets import SYMBOL_NAMES
from analysis import PROFILES

def report(results,profile,connected,elapsed,live_pairs=None,feed_code=None):
    stamp=datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    lines=['📡 <b>Abood Market Radar</b>',f'🕒 {stamp} UTC+3',
      f'الأفق: <b>{PROFILES[profile]["label"]}</b> | الفحص: {elapsed:.1f}s',
      f'مصادقة الاتصال: {"متصلة" if connected else "غير متصلة — النتائج غير معتمدة"}',
      f'الأسعار الحية: {live_pairs if live_pairs is not None else "غير محدد"}/{len(results)} | الحالة: {escape(feed_code or "غير محدد")}',
      f'نافذة القياس: {PROFILES[profile]["window"]}s؛ خط الأساس الإجمالي: {PROFILES[profile]["baseline"]}s',
      'التصنيف وصف للحركة الأخيرة، وليس اتجاه دخول أو نسبة نجاح.']
    groups=[('orderly','🟢 حركة أكثر انتظاماً'),('mixed','🟡 مختلط — حذر'),('unstable','🔴 اضطراب مرتفع'),('unknown','⚪ بيانات غير كافية / قديمة')]
    for category,label in groups:
        group=[r for r in results if r.category==category]
        lines.append(f'\n<b>{label} ({len(group)})</b>')
        if not group:lines.append('—')
        for r in group:
            lines.append(f'• <b>{escape(SYMBOL_NAMES.get(r.symbol,r.symbol))}</b>: {escape("؛ ".join(r.reasons))}')
    lines.append('\n⚠️ اللون الأخضر لا يضمن بقاء الحركة أو ربح أي صفقة. لا توجد أسعار محاكاة.')
    # Keep Telegram text within 4096 characters without cutting HTML tags.
    pages=[];chunk=[];size=0
    for line in lines:
        if size+len(line)+1>3700:pages.append('\n'.join(chunk));chunk=[];size=0
        chunk.append(line);size+=len(line)+1
    if chunk:pages.append('\n'.join(chunk))
    return pages
