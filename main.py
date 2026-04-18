import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from db.database import init_db
from core.schema_memory import init_schema_memory


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_schema_memory()
    yield


app = FastAPI(
    title="AITL - AI Data Translation Layer",
    description="Converts unstructured documents into AI-ready structured JSON",
    version="1.0.0",
    lifespan=lifespan,
)

allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,https://aitl.vercel.app")
allowed_origins = [origin.strip() for origin in allowed_origins_str.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "AITL"}