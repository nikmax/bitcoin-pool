def submit_accepted(result):
    """Bitcoin Core submitblock returns None/null on accepted blocks."""
    return result is None
