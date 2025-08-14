import os
import time
import hmac
import hashlib
import base64
import secrets
import logging
from fastapi import Request, HTTPException

from dotenv import load_dotenv
load_dotenv(override=False)
# Loglama konfiqurasiyası
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ---- Konfiq (ENV) ----
ADMIN_SIGNING_KEY = os.getenv("ADMIN_SIGNING_KEY")  # MÜTLƏQ: uzun, təsadüfi secret
ADMIN_KEY_ID      = os.getenv("ADMIN_KEY_ID", "root")  # opsional: header-də gələn ID ilə yoxlamaq üçün
CLOCK_SKEW_SEC    = int(os.getenv("ADMIN_CLOCK_SKEW_SEC", "300"))  # ±5 dəq pəncərə
NONCE_TTL_SEC     = int(os.getenv("ADMIN_NONCE_TTL", "600"))       # 10 dəq replay qorunması
ADMIN_ALLOWLIST   = {ip.strip() for ip in os.getenv("ADMIN_ALLOWLIST", "").split(",") if ip.strip()}  # "1.2.3.4,5.6.7.8"

# ---- Sadə in-memory nonce store (tək worker üçün). Çox replika varsa Redis istifadə et. ----
_nonce_seen: dict[str, float] = {}

def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def _hash_body(body: bytes) -> str:
    return _b64u(hashlib.sha256(body).digest())

def _calc_sig(secret: str, canonical: str) -> str:
    mac = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
    return _b64u(mac)

async def require_admin(request: Request) -> dict:
    """
    HMAC imzalı admin auth.
    Müştəri aşağıdakı header-ləri göndərməlidir:
      X-Nintel-Key:   <ADMIN_KEY_ID>   (opsional, lakin məsləhətlidir)
      X-Nintel-Ts:    <unix_seconds>   (server ±CLOCK_SKEW_SEC pəncərəsində)
      X-Nintel-Nonce: <128-bit random> (tək istifadə)
      X-Nintel-Sign:  <base64url(HMAC-SHA256(canonical))>

    canonical string:
      METHOD \n PATH \n QUERY \n BODY_SHA256 \n TS \n NONCE
    """
    if not ADMIN_SIGNING_KEY:
        raise HTTPException(status_code=500, detail="Admin signing key not configured")

    # IP allowlist (opsional)
    if ADMIN_ALLOWLIST:
        client_ip = (request.client.host if request.client else None) or ""
        if client_ip not in ADMIN_ALLOWLIST:
            logger.warning(f"Unauthorized IP: {client_ip}")
            raise HTTPException(status_code=403, detail="IP not allowed")

    headers = request.headers
    key_id  = headers.get("x-nintel-key", "")
    ts_str  = headers.get("x-nintel-ts")
    nonce   = headers.get("x-nintel-nonce")
    sign    = headers.get("x-nintel-sign")

    if not (ts_str and nonce and sign):
        logger.error("Missing admin auth headers")
        raise HTTPException(status_code=401, detail="Missing admin auth headers")

    # key_id yoxlaması (əgər ADMIN_KEY_ID təyin edilibsə)
    if ADMIN_KEY_ID and key_id and key_id != ADMIN_KEY_ID:
        logger.error(f"Bad admin key id: {key_id}")
        raise HTTPException(status_code=401, detail="Bad admin key id")

    # saat pəncərəsi
    try:
        ts = int(ts_str)
    except Exception:
        logger.error(f"Invalid timestamp: {ts_str}")
        raise HTTPException(status_code=401, detail="Bad timestamp")
    now = int(time.time())
    if abs(now - ts) > CLOCK_SKEW_SEC:
        logger.warning(f"Timestamp out of window: {ts_str}")
        raise HTTPException(status_code=401, detail="Timestamp out of window")

    # replay qorunması (nonce unikallığı)
    # köhnələri təmizlə
    cutoff = now - NONCE_TTL_SEC
    for n, seen_at in list(_nonce_seen.items()):
        if seen_at < cutoff:
            _nonce_seen.pop(n, None)
    # təkrara icazə yoxdur
    if nonce in _nonce_seen:
        logger.warning(f"Replay attack detected: {nonce}")
        raise HTTPException(status_code=401, detail="Replay detected")
    _nonce_seen[nonce] = now

    # canonical string
    method = request.method.upper()
    path   = request.url.path
    query  = request.url.query or ""
    body   = await request.body()
    body_h = _hash_body(body)
    canonical = "\n".join([method, path, query, body_h, ts_str, nonce])

    sig_calc = _calc_sig(ADMIN_SIGNING_KEY, canonical)
    # constant-time compare
    if not hmac.compare_digest(sign, sig_calc):
        logger.error("Bad signature")
        raise HTTPException(status_code=401, detail="Bad signature")

    # Auth OK
    logger.info(f"Admin authenticated successfully with key_id: {key_id}")
    return {"admin": True, "key_id": key_id or ADMIN_KEY_ID}

