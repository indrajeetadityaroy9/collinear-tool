from datetime import datetime
from pydantic import BaseModel, EmailStr, SecretStr, field_serializer

class RegisterIn(BaseModel):
    email: EmailStr
    password: SecretStr

class LoginIn(BaseModel):
    email: EmailStr
    password: SecretStr

class RefreshIn(BaseModel):
    refresh_token: str

class UpdateUserIn(BaseModel):
    email: EmailStr | None = None
    password: SecretStr | None = None

    @field_serializer("password")
    def _hide_password(self, v: SecretStr | None):
        return "<redacted>" if v else None

class SessionOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
