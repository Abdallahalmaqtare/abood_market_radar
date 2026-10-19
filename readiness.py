"""Transparent readiness diagnostics; never lowers analysis thresholds."""
import math,time
from collections import Counter
from analysis import evaluate,PROFILES

def inspect_symbol(symbol,points,profile,now,mono,stale):
    result=evaluate(symbol,points,profile,now,mono,stale)
    p=PROFILES[profile];step=p['step'];end=int(now//step)-1
    recent_lo=end-int(p['window']/step)+1
    base_lo=end-int(p['baseline']/step)+1
    buckets={int(t.timestamp//step) for t in points if now-p['baseline']-step<=t.timestamp<=now}
    recent=sum(recent_lo<=i<=end for i in buckets)
    base=sum(base_lo<=i<recent_lo for i in buckets)
    recent_target=math.ceil(.8*p['window']/step)
    base_target=math.ceil(.8*(p['baseline']-p['window'])/step)
    span=max(0,min(p['baseline'],now-points[0].timestamp)) if points else 0
    if result.category!='unknown':state='ready'
    elif not points:state='no_data'
    elif result.age>stale:state='stale'
    elif any(('تهيئة' in reason or 'تغطية' in reason or reason=='خط أساس مستقل غير كافٍ') for reason in result.reasons):state='warming'
    else:state='quality_blocked'
    # ETA assumes continuous future feed; it is NOT a countdown guarantee.
    eta=math.ceil(max(0,p['baseline']-span)) if state=='warming' else None
    detail=dict(symbol=symbol,state=state,span_seconds=round(span,1),recent=recent,baseline=base,
        recent_target=recent_target,baseline_target=base_target,eta_seconds=eta,
        progress_percent=100 if state=='ready' else min(99,round(100*span/p['baseline'])),reason='؛ '.join(result.reasons))
    return result,detail

def inspect_snapshot(snapshot,profile,now=None,mono=None,stale=5,connected=True):
    now=time.time() if now is None else now;mono=time.monotonic() if mono is None else mono
    results=[];details=[]
    for symbol,points in snapshot.items():
        result,detail=inspect_symbol(symbol,points,profile,now,mono,stale)
        if not connected:
            result.category='unknown';result.reasons=['البث غير متصل؛ لا اعتماد للبيانات']
            detail.update(state='disconnected',eta_seconds=None,reason=result.reasons[0])
        results.append(result);details.append(detail)
    counts=Counter(d['state'] for d in details)
    etas=[d['eta_seconds'] for d in details if d['state']=='warming' and d['eta_seconds'] is not None]
    return results,dict(details=details,counts=dict(counts),eta_seconds=max(etas,default=None))

def progress_text(summary,profile,live_pairs,feed_code,elapsed):
    from html import escape
    c=summary['counts'];eta=summary['eta_seconds'];details=summary['details']
    lines=['📡 <b>Abood Market Radar</b>','⏳ <b>الطلب مسجّل — جارٍ تجميع العينة، وليس نتيجة فحص</b>',
      'الأفق: '+PROFILES[profile]['label'],
      f'أسعار حديثة: {live_pairs}/{len(details)} | جاهز للتصنيف: {c.get("ready",0)}',
      f'قيد التهيئة: {c.get("warming",0)} | دون أسعار: {c.get("no_data",0)} | قديم: {c.get("stale",0)}',
      f'الحالة: {escape(feed_code)} | مضى على الطلب: {elapsed:.0f}s']
    if eta is not None:lines.append(f'المتبقي التقريبي المحافظ: {eta}s، بشرط استمرار الأسعار بلا انقطاع.')
    if c.get('disconnected'):lines.append('الاتصال غير متاح حالياً؛ إعادة الاتصال التلقائية مستمرة.')
    for d in [d for d in details if d['state']=='warming'][:3]:
        lines.append(f'• {escape(d["symbol"])}: عينة حديثة {d["recent"]}/{d["recent_target"]}، خط أساس {d["baseline"]}/{d["baseline_target"]}')
    lines+=['ستصلك النتيجة تلقائياً عند كفاية العينة؛ لا تضغط الفحص مجدداً.',
      'إعادة تشغيل الخدمة تمسح عينة الذاكرة وتعيد التهيئة.']
    return '\n'.join(lines)
