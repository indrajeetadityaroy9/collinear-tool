from fastapi import FastAPI, Request
import sys
import os

# Create a simple FastAPI app for Vercel
app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Welcome to Collinear API"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# For Vercel
from mangum import Mangum
handler = Mangum(app) 