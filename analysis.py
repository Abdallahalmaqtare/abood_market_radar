"""Explainable, uncalibrated descriptive rules, NOT a probability of trade success.
Time-normalized returns prevent tick-rate bias. Never forward-fill through gaps.
"""
import math, statistics, time
from dataclasses import dataclass,field

PROFILES={
 'fast':dict(label='5–30 ثانية',step=1,window=60,baseline=180,min_recent=40,min_base=100),
 'slow':dict(label='1–5 دقائق',step=5,window=300,baseline=600,min_recent=40,min_base=100),
}
@dataclass
class Result:
    symbol:str
    category:str='unknown'
    reasons:list=field(default_factory=list)
    metrics:dict=field(default_factory=dict)
    samples:int=0
    age:float=0

def evaluate(symbol,points,profile,now=None,mono=None,stale=5):
    p=PROFILES[profile];now=time.time() if now is None else now;mono=time.monotonic() if mono is None else mono
    r=Result(symbol)
    if not points:r.reasons=['لم تصل تكات حقيقية'];return r
    r.age=max(now-points[-1].timestamp,mono-points[-1].received,0)
    if r.age>stale:r.reasons=['بيانات قديمة أو اتصال منقطع'];return r
    step=p['step'];end=int(now//step)-1
    # Close for each occupied time bucket, no historical injection or forward fill.
    buckets={int(t.timestamp//step):t.price for t in points if now-p['baseline']-step<=t.timestamp<=now}
    recent_lo=end-int(p['window']/step)+1
    base_lo=end-int(p['baseline']/step)+1
    recent=[(i,buckets[i]) for i in sorted(buckets) if recent_lo<=i<=end]
    baseline=[(i,buckets[i]) for i in sorted(buckets) if base_lo<=i<recent_lo]
    r.samples=len(recent)
    if len(recent)<p['min_recent'] or len(recent)+len(baseline)<p['min_base']:
        r.reasons=['فترة التهيئة أو تغطية العينة غير كافية'];return r
    expected=int(p['window']/step)
    coverage=len(recent)/expected
    base_expected=int((p['baseline']-p['window'])/step)
    if len(baseline)/base_expected<.8:
        r.reasons=['تغطية خط الأساس السابق غير كافية'];return r
    gaps=[(recent[i][0]-recent[i-1][0])*step for i in range(1,len(recent))]
    if coverage<.8 or max(gaps,default=0)>step*3 or end-recent[-1][0]>1:
        r.reasons=['فجوات في تدفق الأسعار؛ لا يمكن تقييم الاستقرار'];return r
    def returns(rows):
        return [math.log(rows[i][1]/rows[i-1][1]) for i in range(1,len(rows)) if rows[i][0]-rows[i-1][0]==1]
    rs=returns(recent);bs=returns(baseline)
    if len(bs)<20 or len(rs)<30:r.reasons=['خط أساس مستقل غير كافٍ'];return r
    absolute=[abs(x) for x in rs]
    if max(absolute,default=0)<1e-12:r.reasons=['أسعار ثابتة تماماً؛ لا تُصنّف كسوق مستقر'];return r
    rms=lambda a:math.sqrt(sum(x*x for x in a)/max(len(a),1))
    vol=rms(rs);base=rms(bs)
    if base<1e-12:r.reasons=['خط الأساس ساكن؛ المقارنة غير صالحة'];return r
    center=statistics.median(bs)
    mad=statistics.median(abs(x-center) for x in bs)
    threshold=max(6*1.4826*mad,4*base,1e-10)
    spikes=sum(abs(x-center)>threshold for x in rs)/len(rs)
    # Ignore microscopic sign flips below 10% of baseline RMS.
    signs=[1 if x>0 else -1 for x in rs if abs(x)>base*.1]
    reversals=sum(a!=b for a,b in zip(signs,signs[1:]))/max(1,len(signs)-1)
    efficiency=abs(sum(rs))/max(sum(absolute),1e-12)
    ratio=vol/base
    metrics={'volatility_ratio':ratio,'spike_fraction':spikes,'reversals':reversals,
             'efficiency':efficiency,'coverage':coverage,'rms_bps':vol*10000}
    r.metrics={k:round(v,4) for k,v in metrics.items()}
    if spikes>=.05 or ratio>=2.5 or (reversals>=.65 and efficiency<.15):
        r.category='unstable'
        if spikes>=.05:r.reasons.append('قفزات كبيرة متكررة مقارنة بخط الأساس')
        if ratio>=2.5:r.reasons.append('ارتفاع حاد في التذبذب النسبي')
        if reversals>=.65 and efficiency<.15:r.reasons.append('انعكاسات كثيرة ومسار مشوش')
    elif ratio<=1.5 and spikes<=.02 and reversals<=.4 and efficiency>=.2:
        r.category='orderly';r.reasons=['مسار أكثر انتظاماً وقفزات محدودة في العينة']
    else:
        r.category='mixed';r.reasons=['مؤشرات مختلطة؛ لا انتظام واضح ولا اضطراب حاد']
    return r
