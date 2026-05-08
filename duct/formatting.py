"""Text formatting utilities for console and log output."""

import textwrap


def wrap_log(message: str, width: int = 90) -> str:
    """Wrap a message for console logging with a [duct] prefix on the first line.

    Args:
        message: The message to wrap.
        width: Maximum line width (default 90).

    Returns:
        The wrapped message with [duct] prefix on the first line.
    """
    wrapper = textwrap.TextWrapper(
        width=width,
        initial_indent='[duct] ',
        subsequent_indent='       '
    )
    return wrapper.fill(message)


def format_summary(title: str, details: list[str]) -> str:
    """Format a multi-line summary block with a title and indented detail lines.

    Args:
        title: Summary section title (e.g. "Pipeline Summary").
        details: List of detail lines to include under the title.

    Returns:
        A formatted multi-line string.
    """
    lines = [f"=== {title} ==="]
    for d in details:
        lines.append(textwrap.indent(textwrap.fill(d, width=70), '  '))
    return '\n'.join(lines)