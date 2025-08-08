# app/api/auth_deps.py
from typing import Optional, Dict
from fastapi import Header, HTTPException
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
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict:
    """
    Qəbul edilən formalar:
      - X-API-Key: <STATIC_TOKEN>
      - Authorization: Bearer <STATIC_TOKEN>
      - Authorization: Bearer <HS256 JWT>
    Uğurlu olduqda claims dict (və ya minimal info) qaytarır.
    """
    # 1) STATIC_TOKEN
    if settings.static_token:
        if x_api_key and x_api_key == settings.static_token:
            return {"sub": "static-key", "method": "api_key"}
        b = _bearer_from_authorization(authorization)
        if b and b == settings.static_token:
            return {"sub": "static-key", "method": "bearer-static"}

    # 2) HS256 JWT
    token = _bearer_from_authorization(authorization)
    if token and settings.jwt_secret:
        try:
            # aud/iss yoxlamasını hələlik söndürürük — digər backend-lə koordinasiyadan sonra aça bilərik
            claims = jwt.decode(
                token,
                settings.jwt_secret,
                algorithms=["HS256"],
                options={"verify_aud": False, "verify_iss": False},
            )
            # Minimum normalizasiya (istəsən burada roles/scopes yoxlaması da əlavə edə bilərik)
            if "sub" not in claims:
                claims["sub"] = "unknown"
            return claims
        except JWTError:
            raise HTTPException(status_code=401, detail="Token etibarsız və ya vaxtı bitib")

    # 3) Heç biri deyilsə → 401
    raise HTTPException(status_code=401, detail="Authorization tələb olunur")
