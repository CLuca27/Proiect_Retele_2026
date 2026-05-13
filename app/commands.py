def process_message(key: str, payload: bytes) -> str:
    text = payload.decode("utf-8", errors="replace")

    if key == "uppercase":
        return text.upper()

    if key == "reverse":
        return text[::-1]

    if key == "count":
        return str(len(text))

    raise ValueError(f"No command configured for key={key}")
