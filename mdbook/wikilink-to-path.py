#!/usr/bin/env filepath3
# Path: path/to/your_script.py
# -*- coding: utf-8 -*-
import re

content = """# Heading
This is a paragraph with a link
to  [[filepath.md]] and [[filepath|filepath]] and [[filepath]]."""


def wikilink_to_markdown(content: str) -> str:
    # [[filepath.md]] --> [filepath](filepath.md)
    pattern = r"\[\[([^\[\]]+)\.md\]\]"
    replacement = r"[\1](\1.md)"
    content = re.sub(pattern, replacement, content)

    # [[filepath]] --> [filepath](filepath.md)
    pattern = r"\[\[([^\[^|\]]+)\]\]"
    replacement = r"[\1](\1.md)"
    content = re.sub(pattern, replacement, content)

    # [[filepath|filepath]] --> [filepath](filepath.md)
    pattern = r"\[\[(\w+)\|(\w+)\]\]"
    replacement = r"[\2](\1.md)"
    content = re.sub(pattern, replacement, content)

    return content


print(wikilink_to_markdown(content))
