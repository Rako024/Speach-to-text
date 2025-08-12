# app/api/auth_deps.py
from typing import Optional, Dict, Any
from fastapi import Header, HTTPException, Query
from jose import jwt, JWTError

from app.config import Settings

settings = Settings()


def _bearer_from_authorization(header: Optional[str]) -> Optional[str]:
    if not header:
        return None
    parts = header.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def require_auth(
    # Headers
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_api_key: Optional[str]     = Header(default=None, alias="X-API-Key"),
    # Query (video elementləri üçün header verə bilmirik, ona görə bura)
    access: Optional[str]        = Query(default=None, description="Access token (STATIC_TOKEN və ya HS256 JWT)"),
    token_q: Optional[str]       = Query(default=None, alias="token"),
    apikey_q: Optional[str]      = Query(default=None, alias="apikey"),
    access_token_q: Optional[str]= Query(default=None, alias="access_token"),  # <-- ƏLAVƏ OLDU
) -> Dict[str, Any]:
    """
    Qəbul edilən formalar:
      • Header: X-API-Key: <STATIC_TOKEN>
      • Header: Authorization: Bearer <STATIC_TOKEN|JWT>
      • Query : ?access=..., ?token=..., ?access_token=..., ?apikey=...

    Uğurlu olduqda claims dict qaytarır.
    """
    static_token = (settings.static_token or "").strip()
    jwt_secret   = (settings.jwt_secret or "").strip()

    bearer      = _bearer_from_authorization(authorization)
    query_token = (access or token_q or access_token_q or "").strip()   # <-- access_token də daxil
    query_apiky = (apikey_q or "").strip()

    # 1) STATIC_TOKEN
    if static_token:
        if (x_api_key and x_api_key.strip() == static_token) \
           or (bearer and bearer == static_token) \
           or (query_apiky and query_apiky == static_token) \
           or (query_token and query_token == static_token):
            return {"sub": "static-key", "method": "static", "scopes": ["*"]}

    # 2) HS256 JWT
    candidate_jwt = bearer or query_token
    if candidate_jwt and jwt_secret:
        try:
            claims = jwt.decode(
                candidate_jwt,
                jwt_secret,
                algorithms=["HS256"],
                options={"verify_aud": False, "verify_iss": False},
            )
            if "sub" not in claims:
                claims["sub"] = "unknown"
            claims.setdefault("method", "jwt")
            return claims
        except JWTError:
            raise HTTPException(status_code=401, detail="Token etibarsız və ya vaxtı bitib")

    # 3) Heç biri uyğun gəlmirsə
    raise HTTPException(status_code=401, detail="Authorization tələb olunur")
