#!/usr/bin/env python3
# Path: path/to/your_script.py
# -*- coding: utf-8 -*-

import json
import os
import sys
import re
from tqdm import tqdm
from utils import create_title, build_backlinks, wikilink_to_markdown

# Change directory to file location
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


HOME = os.getenv("HOME")
assert HOME is not None, "HOME environment variable not set"
notes_dir = f"{HOME}/Notes/slipbox"


# Get the graph
backlinks_dict = build_backlinks(notes_dir)


def get_backlinks(notes_dir: str, note_relpath: str) -> str:
    # Get the backlinks
    try:
        bl = backlinks_dict[note_relpath]
    except Exception:
        return ""
    bl = {os.path.relpath(d, notes_dir) for d in bl}
    # Format the backlinks (TODO do a markdown link here)
    bl = {f"- [[{d}]]\n" for d in bl}
    bl = "\n\n ## Backlinks\n\n" + "".join(bl)
    return bl


old_text = """ asldkjlk blal [[../bar/baz/foo.bar.the-foo-bar-baz.md]] lskdfjas """


def replace_links(match):
    filepath = match.group(1)
    # remove .md extension and get last part of path
    title = create_title(filepath)
    return f"[{title}]({filepath})"


pattern = r"\[\[(.*?\.md)\]\]"
new_text = re.sub(pattern, replace_links, old_text)



if __name__ == "__main__":
    if len(sys.argv) > 1:  # we check if we received any argument
        if sys.argv[1] == "supports":
            # then we are good to return an exit status code of 0, since the other argument will just be the renderer's name
            sys.exit(0)

    # load both the context and the book representations from stdin
    context, book = json.load(sys.stdin)
    # with open('book.json', 'w') as f:
    #     json.dump(json.load(sys.stdin), f)
    # and now, we can just modify the content of the first chapter

    for i in tqdm(range(len(book["sections"])), desc="Adding Backlinks"):
        for k, v in book["sections"][i].items():
            if k == "Chapter":
                # Use a function to overwrite the content later
                def overwrite_content(content):
                    book["sections"][i][k]["content"] = wikilink_to_markdown(content)

                # Get the old content
                content = book["sections"][i][k]["content"]
                # Get Backlinks
                file_path = book["sections"][i][k]["source_path"]
                content += get_backlinks(notes_dir, file_path)

                # Overwrite the content
                overwrite_content(content)

    # book['sections'][0]['Chapter']['content'] = '# Hello'
    # we are done with the book's modification, we can just print it to stdout,
    print(json.dumps(book))
    # print(book.keys())
