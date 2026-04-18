import os
from sqlalchemy import create_engine, Column, String, JSON, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("WARNING: DATABASE_URL not set. Using local SQLite database (aitl.db).")
    DATABASE_URL = "sqlite:///aitl.db"
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Document(Base):
    __tablename__ = "documents"

    document_id = Column(String, primary_key=True)
    source_file = Column(String)
    document_type = Column(String)
    status = Column(String) # processing, completed, failed
    raw_text = Column(Text)
    structured_output = Column(JSON)
    error_message = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class SchemaMemory(Base):
    __tablename__ = "schema_memory"
    
    signature = Column(String, primary_key=True)
    mapping = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

def init_db():
    Base.metadata.create_all(bind=engine)
