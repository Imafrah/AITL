from parsers.txt_parser import parse_txt, ParseError
from parsers.csv_parser import parse_csv
from parsers.pdf_parser import parse_pdf

SUPPORTED_TYPES = {"txt", "csv", "pdf"}

def route_file(file_bytes: bytes, file_type: str) -> dict:
    if file_type not in SUPPORTED_TYPES:
        raise ParseError(f"Unsupported file type: {file_type}")

    if file_type == "txt":
        return parse_txt(file_bytes)
    elif file_type == "csv":
        return parse_csv(file_bytes)
    elif file_type == "pdf":
        return parse_pdf(file_bytes)