"""Independent read-only radar configuration; never imports the trading bot."""
import json, os
from dataclasses import dataclass
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv('/etc/secrets/.env', override=False)
load_dotenv(override=False)

@dataclass
class Config:
    token: str
    admin: int
    auth: str
    url: str
    port: int = 10000
    scan_seconds: float = 20
    stale_seconds: float = 5
    tick_capacity: int = 30000
    retention_seconds: int = 900
    stream_timeout: float = 45
    timestamp_offset: str = "auto"
    subscription_mode: str = "browser"
    auto_wait_seconds: float = 660
    progress_seconds: float = 15

    @classmethod
    def load(cls):
        token=os.getenv('RADAR_BOT_TOKEN','').strip()
        try: admin=int(os.getenv('ADMIN_TELEGRAM_ID','0'))
        except ValueError: raise ValueError('ADMIN_TELEGRAM_ID must be a positive integer') from None
        raw=os.getenv('PO_SSID','').strip()
        if not token or admin <= 0 or not raw:
            raise ValueError('Set RADAR_BOT_TOKEN, ADMIN_TELEGRAM_ID and PO_SSID')
        try:
            data=json.loads(raw[2:] if raw.startswith('42') else raw)
            if isinstance(data,dict): data=['auth',data]
            if not isinstance(data,list) or len(data)!=2 or data[0]!='auth' or not isinstance(data[1],dict): raise ValueError()
            payload=data[1]
            if not isinstance(payload.get('session'),str) or len(payload['session'])<10 or '...' in payload['session']: raise ValueError()
            if not isinstance(payload.get('uid'),int) or isinstance(payload['uid'],bool) or payload['uid']<=0: raise ValueError()
            if payload.get('isDemo') not in (0,1): raise ValueError()
            auth='42'+json.dumps(data,separators=(',',':'))
        except (ValueError,TypeError,KeyError):
            raise ValueError('PO_SSID must be a complete auth frame with actual session, positive uid and isDemo') from None
        url=os.getenv('PO_WS_URL','wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket').strip()
        parsed=urlparse(url)
        if parsed.scheme!='wss' or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError('PO_WS_URL must be a secure wss URL without userinfo')
        c=cls(token,admin,auth,url,int(os.getenv('PORT','10000')),
            float(os.getenv('SCAN_SECONDS','20')),float(os.getenv('STALE_SECONDS','5')),
            int(os.getenv('TICK_CAPACITY','30000')),int(os.getenv('RETENTION_SECONDS','900')),
            float(os.getenv('STREAM_TIMEOUT_SECONDS','45')))
        if not 1<=c.port<=65535 or not 1<=c.scan_seconds<=50 or not 1<=c.stale_seconds<=15:
            raise ValueError('Invalid port, scan duration or freshness threshold')
        if not 1000<=c.tick_capacity<=100000 or not 600<=c.retention_seconds<=3600 or not 10<=c.stream_timeout<=180:
            raise ValueError('Invalid capacity, retention or stream timeout')
        c.timestamp_offset=os.getenv('PO_TIMESTAMP_OFFSET_SECONDS','auto').strip().lower()
        if c.timestamp_offset!='auto':
            try:
                value=float(c.timestamp_offset)
                if not __import__('math').isfinite(value) or abs(value)>43200:raise ValueError()
            except ValueError:raise ValueError('Invalid PO_TIMESTAMP_OFFSET_SECONDS') from None
        c.subscription_mode=os.getenv('PO_SUBSCRIPTION_MODE','browser').strip().lower()
        if c.subscription_mode not in ('browser','subscribe_only'):raise ValueError('Invalid PO_SUBSCRIPTION_MODE')
        c.auto_wait_seconds=float(os.getenv('AUTO_WAIT_SECONDS','660'))
        c.progress_seconds=float(os.getenv('PROGRESS_SECONDS','15'))
        import math
        if not math.isfinite(c.auto_wait_seconds) or not 0<=c.auto_wait_seconds<=900:raise ValueError('Invalid AUTO_WAIT_SECONDS')
        if not math.isfinite(c.progress_seconds) or not 5<=c.progress_seconds<=60:raise ValueError('Invalid PROGRESS_SECONDS')
        return c
