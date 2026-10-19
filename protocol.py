"""Socket.IO JSON and binary-JSON attachment decoder. No order emit methods."""
import json

class Decoder:
    def __init__(self): self.pending=None
    def feed(self,frame):
        try:
            if isinstance(frame,bytes):
                if self.pending is None: return []
                event,count,items=self.pending
                items.append(json.loads(frame.decode('utf-8')))
                if len(items)<count:return []
                self.pending=None
                def restore(x):
                    if isinstance(x,dict) and x.get('_placeholder') is True: return items[x['num']]
                    if isinstance(x,dict):return {k:restore(v) for k,v in x.items()}
                    if isinstance(x,list):return [restore(v) for v in x]
                    return x
                return [restore(event)]
            if frame.startswith('45') and '-' in frame:
                number,data=frame[2:].split('-',1);count=int(number)
                if not 1<=count<=10: raise ValueError()
                self.pending=(json.loads(data),count,[]);return []
            if frame.startswith('42'):return [json.loads(frame[2:])]
        except (ValueError,TypeError,IndexError,KeyError,UnicodeDecodeError):self.pending=None
        return []

def ticks(payload):
    """Only updateStream payloads are passed here, never history/candle events."""
    if isinstance(payload,dict):
        symbol=payload.get('asset',payload.get('symbol'))
        stamp=payload.get('timestamp',payload.get('time'))
        price=payload.get('value',payload.get('price',payload.get('close')))
        if symbol is not None and stamp is not None and price is not None:
            yield symbol,stamp,price
        else:
            for key in ('data','ticks','assets'):
                if isinstance(payload.get(key),(list,dict)):yield from ticks(payload[key])
    elif isinstance(payload,list):
        if len(payload)>=3 and isinstance(payload[0],str):yield payload[0],payload[1],payload[2]
        else:
            for row in payload:yield from ticks(row)
