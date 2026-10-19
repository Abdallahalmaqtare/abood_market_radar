"""Read-only Socket.IO decoder. Never infer stream events from unlabeled prices."""
import json,re
from collections import Counter

def unpack(value):
    for _ in range(3):
        if not isinstance(value,(str,bytes)):break
        try:value=json.loads(value)
        except (ValueError,UnicodeDecodeError):break
    return value

class Decoder:
    def __init__(self):self.pending=None;self.stats=Counter()
    def _restore(self,event,items):
        def restore(x):
            if isinstance(x,dict) and x.get('_placeholder') is True:
                n=x.get('num')
                if not isinstance(n,int) or n<0 or n>=len(items):raise ValueError()
                return items[n]
            if isinstance(x,dict):return {k:restore(v) for k,v in x.items()}
            if isinstance(x,list):return [restore(v) for v in x]
            return x
        return restore(event)
    def feed(self,frame):
        self.stats['frames']+=1
        try:
            binary=isinstance(frame,bytes)
            if binary:
                self.stats['binary_frames']+=1
                frame=frame.decode('utf-8')
            if not isinstance(frame,str):return []
            # Socket.IO attachments may be binary JSON or JSON strings.
            if self.pending and (binary or frame.lstrip().startswith(('[','{','"'))):
                event,count,items=self.pending;items.append(unpack(json.loads(frame)))
                if len(items)<count:return []
                self.pending=None;self.stats['events']+=1
                return [self._restore(event,items)]
            m=re.fullmatch(r'45(\d+)-(?:/[^,]*,)?\d*(\[.*)',frame,re.S)
            if m:
                count=int(m[1])
                if not 1<=count<=10:raise ValueError()
                self.pending=(json.loads(m[2]),count,[]);return []
            m=re.fullmatch(r'42(?:/[^,]*,)?\d*(\[.*)',frame,re.S)
            if m:
                self.stats['events']+=1;return [json.loads(m[1])]
            # Standalone binary event envelope is safe; naked tick arrays are not.
            obj=unpack(frame)
            if isinstance(obj,list) and len(obj)>=2 and isinstance(obj[0],str) and isinstance(obj[1],(dict,list,str)):
                if obj[0] in ('updateStream','successauth','successupdateBalance','updateBalance','updateAssets','failauth','errorauth','authError'):
                    self.stats['events']+=1;return [obj]
        except (ValueError,TypeError,IndexError,KeyError,UnicodeDecodeError):
            self.pending=None;self.stats['decode_errors']+=1
        return []

def ticks(payload,depth=0):
    if depth>5:return
    payload=unpack(payload)
    if isinstance(payload,dict):
        symbol=payload.get('asset',payload.get('symbol'))
        stamp=payload.get('timestamp',payload.get('time'))
        price=payload.get('value',payload.get('price',payload.get('close')))
        if symbol is not None and stamp is not None and price is not None:yield symbol,stamp,price
        else:
            for key in ('data','ticks','assets'):
                if key in payload:yield from ticks(payload[key],depth+1)
    elif isinstance(payload,list):
        if len(payload)>=3 and isinstance(payload[0],str):yield payload[0],payload[1],payload[2]
        else:
            for row in payload:yield from ticks(row,depth+1)
