from pathlib import Path


def route_file(file_name: str) -> str:
    """
    Classify file by extension for pipeline selection.

    Returns:
        "structured" — CSV (tabular / schema memory path)
        "unstructured" — TXT or PDF (text extraction + AI)
        "unknown" — unsupported extension
    """
    if not file_name or not str(file_name).strip():
        return "unknown"
    suffix = Path(str(file_name).strip()).suffix.lower()
    if suffix == ".csv":
        return "structured"
    if suffix in (".txt", ".pdf"):
        return "unstructured"
    return "unknown"
