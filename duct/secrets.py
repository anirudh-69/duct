"""Utilities for generating secure identifiers."""

import secrets


def generate_run_id() -> str:
    """Generate a cryptographically random 16-character hexadecimal run ID.

    Returns:
        A unique run identifier string (e.g. "a1b2c3d4e5f60718").
    """
    return secrets.token_hex(8)