#!/usr/bin/env python3
# Path: path/to/your_script.py
# -*- coding: utf-8 -*-

from collections import defaultdict
import re
import os
import subprocess


def create_title(filename: str, with_ext: bool = True) -> str:
    if with_ext:
        filename, _ = os.path.splitext(filename)
    # Strip the leading ./ if it's there
    if filename.startswith("./"):
        filename = filename[2:]
    title = filename
    # Remove Parent directories (It's much simpler this way)
    # Go flat directories and use dots, life is too short for hierarchies
    # and the BS complexity they cause for linking
    title = os.path.basename(filename)
    # protect ../
    # Deal with snake case
    title = title.replace("_", " ")
    # Remove kebab case
    title = title.replace("-", " ")
    # Represent Heiraarchical structure
    title = title.replace(".", "/")
    # Make sentence case
    title = title.capitalize()
    # Put the ../ back
    return title


def create_target(filename: str) -> str:
    """Create a target for the markdown link"""
    filename = filename.replace(" ", "%20")
    return filename


def create_link(filename: str, add_extension: bool = False) -> str:
    title = create_title(filename)
    target = create_target(filename)
    ext = "" if not add_extension else ".md"
    return f"- [{title}]({target}{ext})"


def get_backlinks(file: str) -> list[str] | None:
    """Get backlinks for a file
    :param file: The file to get backlinks for
    :type file: str
    :return: A list of files that link to the file
    :rtype: list[str] | None

    Note: This should only be used for a single file, if backlinks
    are required for multiple files, use the mdbook-backlinks.build_backlinks
    it will be more performant, efficient and all python no shell

    """
    try:
        out = subprocess.run(
            ["rg", "-l", file, "./src"], stdout=subprocess.PIPE, text=True, check=True
        )
    except Exception as e:
        # print(e, file=sys.stderr)
        return None
    files = [os.path.basename(f) for f in out.stdout.split("\n")]
    files = [f for f in files if f not in ["", "SUMMARY.md"]]
    return files


def build_backlinks(notes_dir: str) -> dict:
    # Pattern to get markdown links
    link_pattern = re.compile(r"\[.*?\]\((.*?)\.md\)")

    # empty dictionary
    backlinks = defaultdict(set)

    # Loop over all files in the notes directory
    for root, _, files in os.walk(notes_dir):
        for file in files:
            # Only process markdown files as extension is hardcoded
            if file.endswith(".md"):
                file_path = os.path.join(root, file)
                # Remove unwanted stuff
                for bad in [".unison.", ".DS_Store", "sync-conflict"]:
                    if bad in file_path:
                        continue
                # Open the file
                with open(file_path, "r") as f:
                    # Get the links
                    content = f.read()
                    links = link_pattern.findall(content)
                    # Add each link to the backlinks dictionary
                    for link in links:
                        # Add but don't split on whitespace
                        backlinks[link + ".md"].add(file_path.replace(" ", "%20"))

    # Remove entries that aren't under the notes directory
    backlinks = {file: backlinks[file] for file in backlinks if file in backlinks}

    return backlinks


# NOTE this is identical as the above function but takes a list of files
# I should refactor the two to work together, however
# I would have to build the list then re-iterate and I'm concerned about
# performance and honestly cbf.
def build_backlinks_from_files(files: list[str]) -> dict:
    # Pattern to get markdown links
    link_pattern = re.compile(r"\[.*?\]\((.*?)\.md\)")

    # empty dictionary
    backlinks = defaultdict(set)

    for file_path in files:
        # Remove unwanted stuff
        for bad in [".unison.", ".DS_Store", "sync-conflict"]:
            if bad in file_path:
                continue
        # Open the file
        with open(file_path, "r") as f:
            # Get the links
            content = f.read()
            links = link_pattern.findall(content)
            # Add each link to the backlinks dictionary
            for link in links:
                # Add but don't split on whitespace
                backlinks[link + ".md"].add(file_path.replace(" ", "%20"))

    # Remove entries that aren't under the notes directory
    backlinks = {file: backlinks[file] for file in backlinks if file in backlinks}

    return backlinks


# If allowing spaces in file names hash the filename before using as a key
# Life is too short for spaces in file names
def file_key(f: str):
    return hash(f)


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
