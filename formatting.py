import textwrap

def wrap_log(message: str, width: int = 90) -> str:
    wrapper = textwrap.TextWrapper(
        width=width,
        initial_indent='[duct] ',
        subsequent_indent='       '
    )
    return wrapper.fill(message)

def format_summary(title: str, details: list[str]) -> str:
    lines = [f"=== {title} ==="]
    for d in details:
        lines.append(textwrap.indent(textwrap.fill(d, width=70), '  '))
    return '\n'.join(lines)