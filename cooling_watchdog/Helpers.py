def window_triggers_label_from_str(triggers_str: str) -> str:
    """
    Normalize a triggers string:
    - Split by comma
    - Strip spaces
    - Deduplicate
    - Title case
    - Sort alphabetically
    """
    if not triggers_str:
        return ""
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}
    uniq = uniq & allowed  # ignore typos/unexpected tokens
    return ", ".join(sorted(uniq))

def risk_score_simple(triggers_str: str) -> int:
    """
    Score = number of distinct trigger types present (0..3).
    """
    if not triggers_str:
        return 0
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}
    return min(3, len(uniq & allowed))
