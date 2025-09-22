from fastapi import FastAPI
import uvicorn


app = FastAPI(title="Collinear API")

@app.get("/")
async def root():
    return {"message": "Welcome to Collinear API"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":

    uvicorn.run("app:app", host="0.0.0.0", port=7860, reload=True)