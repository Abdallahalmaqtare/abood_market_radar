import asyncio, json, logging, math, random, time
from collections import deque
from dataclasses import dataclass
from contextlib import suppress
import websockets
from assets import SYMBOL_NAMES
from protocol import Decoder,ticks
log=logging.getLogger('radar.market')

@dataclass(frozen=True)
class Tick:
    timestamp: float
    price: float
    received: float

class Store:
    def __init__(self,capacity=30000,retention=900):
        self.data={s:deque(maxlen=capacity) for s in SYMBOL_NAMES}
        self.retention=retention
        self.accepted=0;self.rejected=0
    def clear(self):
        for q in self.data.values():q.clear()
    def add(self,symbol,stamp,price,wall=None,mono=None):
        if symbol not in self.data:return False
        wall=time.time() if wall is None else wall
        mono=time.monotonic() if mono is None else mono
        try:
            stamp=float(stamp);price=float(price)
            if stamp>1e11:stamp/=1000
            if not math.isfinite(stamp) or not math.isfinite(price) or price<=0 or stamp>wall+3 or wall-stamp>15:raise ValueError()
            q=self.data[symbol]
            if q and stamp<=q[-1].timestamp:raise ValueError()
            q.append(Tick(stamp,price,mono))
            while q and q[0].timestamp<stamp-self.retention:q.popleft()
            self.accepted+=1;return True
        except (TypeError,ValueError,OverflowError):self.rejected+=1;return False
    def snapshot(self):return {s:tuple(q) for s,q in self.data.items()}
    def fresh_count(self,limit=5):
        mono=time.monotonic();wall=time.time()
        return sum(bool(q) and mono-q[-1].received<=limit and wall-q[-1].timestamp<=limit for q in self.data.values())

class Feed:
    def __init__(self,cfg,store):
        self.cfg=cfg;self.store=store;self.connected=False;self.last_error='NOT_CONNECTED'
        self.reconnects=0;self.ws=None;self.last_tick_at=0;self.subscribed=0
    def process(self,event):
        if not isinstance(event,list) or len(event)<2:return
        name,payload=event[:2]
        if name in ('failauth','errorauth','authError'):raise PermissionError('AUTH_REJECTED')
        if name=='updateStream':
            for s,t,p in ticks(payload):
                if self.store.add(s,t,p):self.last_tick_at=time.monotonic()
    async def _session(self):
        self.store.clear();self.last_tick_at=0;self.subscribed=0
        decoder=Decoder();authorized=False
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
                    if f=='2':await ws.send('3')
                    elif isinstance(f,str) and f.startswith('40'):break
                await ws.send(self.cfg.auth)
                while not authorized:
                    frame=await ws.recv()
                    if frame=='2':await ws.send('3');continue
                    for ev in decoder.feed(frame):
                        self.process(ev)
                        if isinstance(ev,list) and len(ev)>1:
                            payload=ev[1]
                            authorized=authorized or ev[0]=='successauth' or (ev[0] in ('successupdateBalance','updateBalance') and isinstance(payload,dict) and 'balance' in payload)
            self.connected=True;self.last_error='NONE'
            async def subscribe():
                # Explicit subscription for every symbol from the supplied project's list.
                for s in SYMBOL_NAMES:
                    await ws.send('42'+json.dumps(['subscribeSymbol',s],separators=(',',':')))
                    self.subscribed+=1
                    await asyncio.sleep(.08)
            task=asyncio.create_task(subscribe())
            started=time.monotonic()
            try:
                while True:
                    if task.done():task.result()
                    frame=await asyncio.wait_for(ws.recv(),timeout=self.cfg.stream_timeout)
                    if frame=='2':await ws.send('3')
                    elif frame=='1':raise ConnectionError('SERVER_CLOSE')
                    else:
                        for ev in decoder.feed(frame):self.process(ev)
                    if time.monotonic()-(self.last_tick_at or started)>self.cfg.stream_timeout:
                        self.last_error='NO_LIVE_TICKS';raise TimeoutError('NO_LIVE_TICKS')
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
                # Never print exception text, URL, raw frames or credentials.
                self.last_error=('AUTH_REJECTED' if isinstance(exc,PermissionError) else
                    'NO_LIVE_TICKS' if self.last_error=='NO_LIVE_TICKS' else
                    'CONNECT_TIMEOUT' if isinstance(exc,TimeoutError) else 'WS_DISCONNECTED')
                log.warning('Feed unavailable: code=%s type=%s',self.last_error,type(exc).__name__)
            finally:self.connected=False;self.ws=None;self.store.clear()
            self.reconnects+=1
            if time.monotonic()-started>60:delay=1
            await asyncio.sleep(delay+random.uniform(0,.5));delay=min(delay*2,30)
