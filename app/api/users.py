from fastapi import APIRouter, Depends
from app.api.dependencies import current_user
from app.models.user import User

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/me", summary="Return the authenticated user")
async def read_current_user(user: User = Depends(current_user)):
    """
    Requires:  Authorization: Bearer <JWT>
    """
    return {"id": user.id, "email": user.email}
