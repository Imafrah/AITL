from utils.data_cleaner import clean_txt_text


class ParseError(Exception):
    pass


def parse_txt(file_bytes: bytes) -> dict:
    try:
        try:
            text = file_bytes.decode("utf-8").strip()
        except UnicodeDecodeError:
            text = file_bytes.decode("latin-1").strip()

        if not text:
            raise ParseError("File is empty.")

        text = clean_txt_text(text)

        if not text:
            raise ParseError("File has no usable content after cleaning.")

        word_count = len(text.split())

        return {
            "text": text,
            "metadata": {
                "file_type": "txt",
                "page_count": None,
                "word_count": word_count,
            },
        }

    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Failed to parse TXT: {e}") from e
