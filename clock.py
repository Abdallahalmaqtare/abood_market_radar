"""Normalize recognized quote clocks, never replace market time with arrival time.
AUTO permits UTC or a documented UTC+2 quote clock, not arbitrary offsets.
For a nonzero auto offset each symbol must demonstrate advancing timestamps
across >=1s and >=3 separate arrivals before acceptance.
"""
import math
from collections import Counter
class Clock:
    def __init__(self,setting='auto'):
        self.setting=setting;self.offsets=Counter();self.probes={};self.last_reason='NONE'
    def convert(self,symbol,stamp,wall,mono):
        try:
            stamp=float(stamp)
            if stamp>1e17:stamp/=1e9
            elif stamp>1e14:stamp/=1e6
            elif stamp>1e11:stamp/=1e3
            if not math.isfinite(stamp):raise ValueError()
        except (TypeError,ValueError,OverflowError):self.last_reason='invalid_timestamp';return None
        candidates=(0,-7200) if self.setting=='auto' else (float(self.setting),)
        fits=[o for o in candidates if -3<=wall-(stamp+o)<=15]
        if not fits:
            self.last_reason='clock_out_of_range';return None
        offset=min(fits,key=lambda o:abs(wall-stamp-o))
        if self.setting=='auto' and offset:
            probe=self.probes.get(symbol)
            if probe is None or probe['offset']!=offset or stamp<probe['last'] or mono-probe['last_arrival']>10:
                probe=dict(offset=offset,first=stamp,start=mono,last=stamp,last_arrival=mono,count=1,ready=False)
                self.probes[symbol]=probe
            elif stamp>probe['last'] and mono>probe['last_arrival']:
                elapsed=mono-probe['start'];delta=stamp-probe['first']
                if abs(elapsed-delta)>3:
                    self.probes.pop(symbol,None);self.last_reason='clock_progression_invalid';return None
                probe['count']+=1;probe['last']=stamp;probe['last_arrival']=mono
                if elapsed>=1 and delta>=1 and probe['count']>=3:probe['ready']=True
            if not probe['ready']:self.last_reason='clock_verifying';return None
        self.offsets[offset]+=1;self.last_reason='NONE';return stamp+offset
