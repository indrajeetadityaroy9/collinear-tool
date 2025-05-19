from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from datetime import datetime, timezone, timedelta
from typing import Optional
import bcrypt
from pydantic import BaseModel, EmailStr

from app.services.auth_service import AuthService
from app.core.dependencies import get_auth_service
from app.schemas.user import User
from app.core.supabase import get_cached_supabase_service_client
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

# JWT configuration (pulled from global settings)
# NOTE: ensure `SECRET_KEY` is set via environment variables in production.
SECRET_KEY = settings.SECRET_KEY.get_secret_value()
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

# Token schemas
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenData(BaseModel):
    email: Optional[str] = None

# Login credentials
class LoginCredentials(BaseModel):
    email: EmailStr
    password: str

# Registration credentials
class RegisterCredentials(BaseModel):
    email: EmailStr
    password: str
    name: str = "Test User"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

# Helper functions
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode(), salt)
    return hashed_password.decode()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())

async def get_user_by_email(email: str):
    """Retrieve user by e-mail using a cached service-role Supabase client (no JWT needed)."""
    supabase = get_cached_supabase_service_client()
    response = supabase.table("users", schema="auth").select("*").eq("email", email).execute()
    if response.data and len(response.data) > 0:
        return response.data[0]
    return None

async def authenticate_user(email: str, password: str):
    user = await get_user_by_email(email)
    if not user:
        return False
    if not verify_password(password, user["password"]):
        return False
    return user

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception
        
    user = await get_user_by_email(token_data.email)
    if user is None:
        raise credentials_exception
    return user

# Auth endpoints
@router.post("/login", response_model=Token)
async def login(credentials: LoginCredentials):
    supabase = get_cached_supabase_service_client()
    try:
        res = supabase.auth.sign_in_with_password({
            "email": credentials.email,
            "password": credentials.password
        })
        if not res.session:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token = res.session.access_token
        return {"access_token": access_token, "token_type": "bearer"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")

@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=Token)
async def register(credentials: RegisterCredentials):
    supabase = get_cached_supabase_service_client()
    try:
        res = supabase.auth.sign_up({
            "email": credentials.email,
            "password": credentials.password
        })
        if not res.user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Registration failed or email already registered"
            )
        access_token = res.session.access_token if res.session else None
        return {"access_token": access_token or "", "token_type": "bearer"}
    except Exception as e:
        # If duplicate email, return 400 or 409
        if "User already registered" in str(e) or "already registered" in str(e):
            raise HTTPException(status_code=409, detail="Email already registered")
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.get("/me", response_model=User)
async def get_user_profile(current_user: dict = Depends(get_current_user)):
    # Return user without password
    user_data = {
        "id": current_user["id"],
        "email": current_user["email"],
        "name": current_user.get("name")
    }
    return user_data
