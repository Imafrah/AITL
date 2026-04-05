import pdfplumber
import io
from parsers.txt_parser import ParseError

def parse_pdf(file_bytes: bytes) -> dict:
    try:
        text_parts = []

        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            page_count = len(pdf.pages)

            if page_count == 0:
                raise ParseError("PDF has no pages.")

            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text.strip())

        full_text = "\n\n".join(text_parts).strip()

        if not full_text:
            raise ParseError("Could not extract text from PDF. It may be scanned or image-based.")

        return {
            "text": full_text,
            "metadata": {
                "file_type": "pdf",
                "page_count": page_count,
                "word_count": len(full_text.split())
            }
        }

    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Failed to parse PDF: {e}")