import os
import uuid
from dotenv import load_dotenv
load_dotenv()

from core.universal_pipeline import process_universal

os.environ["AITL_CLEAN_MODE"] = "strict"

def regenerate():
    api_key = os.getenv("GEMINI_API_KEY", "dummy_key")
    sales_path = "sample_data/sales_001.csv"
    if os.path.exists(sales_path):
        with open(sales_path, "rb") as f:
            content = f.read()
            doc_id = str(uuid.uuid4())
            print(f"Processing {sales_path} -> {doc_id}")
            result = process_universal(content, "csv", "sales_001.csv", api_key)
            print("Successfully processed sales data.")
            print(f"Output saved in output/{result.get('document_id')}")

if __name__ == "__main__":
    regenerate()
