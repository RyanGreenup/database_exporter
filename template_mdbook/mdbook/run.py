#!/usr/bin/env python3
# Path: path/to/your_script.py
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
from tqdm import tqdm
import numpy as np
from utils import create_link, build_backlinks_from_files

# TODO refactor the for loop to open each file inside the loop
# but only open  SUMMARY.md once.
# Then use multiprocessing.Pool() to parallelize the process
# https://www.squash.io/how-to-parallelize-a-simple-python-loop/


script_dir = os.path.realpath(os.path.dirname(__file__))
os.chdir(script_dir)


def get_all_links(verbose: bool = False, allow_spaces: bool = False):
    """Get all file links for files underneath the src directory
    This assumes that the current file location is above the src directory,
    this should be the same directory as the `book.toml` file

    This returns a list of relative file paths
    """
    fl = []
    os.chdir(os.path.realpath("src"))
    for root, _, files in tqdm(os.walk(".")):
        for file in files:
            if file.endswith(".md"):
                file = os.path.join(root, file)
                # for file in tqdm(files):
                # Remove extension
                base, ext = os.path.splitext(file)

                # Test the extension
                if ext != ".md":
                    continue

                # Check that the file is not the summary
                if base == "SUMMARY":
                    continue

                # Skip files with spaces in file names or hidden files
                if not allow_spaces:
                    if " " in file:
                        continue

                # Check that the file can be opened
                try:
                    with open(file) as f:
                        _ = f.read()
                except Exception as e:
                    if verbose:
                        print(e, file=sys.stderr)
                    continue

                fl += [file]

    os.chdir(script_dir)
    return fl


def validate_markdown_files(f: str) -> bool:
    # Tdoes it have an extenion?
    _, ext = os.path.splitext(f)
    if ext != ".md":
        return False

    # can we open it?
    try:
        with open(f) as fp:
            _ = fp.read()
    except Exception as e:
        return False

    return True


def sort_links(fl: list[str]):
    """Sort links by pagerank"""
    os.chdir(os.path.realpath("src"))
    graph = build_backlinks_from_files(fl)
    files = fl
    # Validate them to be sure
    files = [f for f in files if validate_markdown_files(f)]

    adjacency_matrix = []
    for file in files:
        links = graph.get(file)
        if links is None:
            links = []
        row = [1 if f in links else 0 for f in files]
        adjacency_matrix.append(row)
    adjacency_matrix = np.array(adjacency_matrix)
    # Can't use eigenvalue for large matrices, use power iteration
    # pagerank = np.linalg.eig(adjacency_matrix)[1]
    p = np.ones(len(files))
    for _ in range(100):
        p = np.dot(adjacency_matrix, p)

    # Sort files by p
    files = np.array(files)
    files = files[np.argsort(p)]

    # # Separate those with no backlinks
    # leftovers = [f for f in fl if f not in files]

    # # Append the leftovers
    # files = files.tolist()
    # files += leftovers

    os.chdir(script_dir)
    # Reverse it so the highest pagerank is first
    return files[::-1]


def write_links_to_summary_file():
    fl = get_all_links(allow_spaces=False)
    fl = sort_links(fl)
    ll = [create_link(f) for f in fl]

    # Declare a separator so the user can have a usable sidebar
    seperator = "\n\n # Table of Contents\n\n\n"

    # Truncate everything after the seperator
    try:
        with open("./src/SUMMARY.md", "r") as f:
            content = f.read()
            content = content.split(seperator)[0]
    except Exception as e:
        content = ""
    content = content + seperator
    content = content + "\n".join(ll)

    with open("./src/SUMMARY.md", "w") as f:
        f.write(content)


if __name__ == "__main__":
    write_links_to_summary_file()

    # Serving the book builds it anyway
    subprocess.run(["mdbook", "build"])
    port = "8881"
    subprocess.run(["mdbook", "serve", "-n", "0.0.0.0", "-p", port])


# Insights
# Much more reliable if directories are flat, not quite sure why some files are not being read
# No backlinks
# Trivial to embed if and only if the files are flat

# Missing Features
# Katex has a high quality crate
# Mermain and admonish are nice (callout would be better)
# NVM there is callouts like obsidian
# https://crates.io/crates/mdbook-callouts
# https://crates.io/crates/mdbook-wikilinks
# https://crates.io/crates/mdbook-backlinks/0.2.7
# https://www.reddit.com/r/techsupport/comments/16mv9n2/how_can_i_use_backlinks_in_mdbook/

# Hard to say how good search is but semantic search locally is best in class
# worst case fight typesense or meilisearch
