import io
import pandas as pd
from parsers.txt_parser import ParseError

def parse_csv(file_bytes: bytes) -> dict:
    try:
        # Try UTF-8 first, fall back to latin-1
        try:
            text_io = io.StringIO(file_bytes.decode("utf-8"))
        except UnicodeDecodeError:
            text_io = io.StringIO(file_bytes.decode("latin-1"))

        df = pd.read_csv(text_io)

        if df.empty:
            raise ParseError("CSV file is empty or has no data rows.")

        # Convert dataframe to readable text for AI
        text = df.to_string(index=False)

        # Also capture column summary
        columns = list(df.columns)
        row_count = len(df)

        return {
            "text": text,
            "metadata": {
                "file_type": "csv",
                "page_count": None,
                "word_count": len(text.split()),
                "row_count": row_count,
                "columns": columns
            }
        }

    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Failed to parse CSV: {e}")