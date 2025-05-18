# app/schemas/user.py
from pydantic import BaseModel, EmailStr

class User(BaseModel):
    """Lightweight user object returned by `current_user` dependency.

    Only the fields you really use elsewhere are kept.
    Add more whenever you need them.
    """
    id: str
    email: EmailStr
