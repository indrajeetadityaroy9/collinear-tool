from fastapi import FastAPI, Request
from app.api import api_router
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.core.exceptions import SupabaseClientError

app = FastAPI(title="Collinear API")

# Enable CORS for the frontend
frontend_origin = "http://localhost:5173"
app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Welcome to the Collinear Data Tool API"}

@app.exception_handler(SupabaseClientError)
async def supabase_error_handler(request: Request, exc: SupabaseClientError):
    return JSONResponse(status_code=500, content={"detail": str(exc)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
