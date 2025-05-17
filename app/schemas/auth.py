from pydantic import BaseModel, EmailStr, SecretStr
from datetime import datetime

class RegisterIn(BaseModel):
    email: EmailStr
    password: SecretStr

class LoginIn(BaseModel):
    email: EmailStr
    password: SecretStr

class SessionOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime