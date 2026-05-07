import secrets

def generate_run_id() -> str:
    return secrets.token_hex(8)