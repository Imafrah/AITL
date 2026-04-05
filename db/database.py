import os
from sqlalchemy import create_engine, Column, String, JSON, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Document(Base):
    __tablename__ = "documents"

    document_id = Column(String, primary_key=True)
    source_file = Column(String)
    document_type = Column(String)
    status = Column(String)
    raw_text = Column(Text)
    structured_output = Column(JSON)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

def init_db():
    Base.metadata.create_all(bind=engine)
