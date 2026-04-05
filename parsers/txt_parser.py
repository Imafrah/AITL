class ParseError(Exception):
    pass

def parse_txt(file_bytes: bytes) -> dict:
    try:
        text = file_bytes.decode("utf-8").strip()
    except UnicodeDecodeError:
        raise ParseError("Could not decode file. Make sure it is a valid UTF-8 text file.")

    if not text:
        raise ParseError("File is empty.")

    word_count = len(text.split())

    return {
        "text": text,
        "metadata": {
            "file_type": "txt",
            "page_count": None,
            "word_count": word_count
        }
    }