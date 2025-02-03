#!/usr/bin/env python3
# Path: path/to/your_script.py
# -*- coding: utf-8 -*-

from pprint import pprint
import os
import re
from collections import defaultdict


def build_backlinks(notes_dir: str) -> dict:
    # Pattern to get markdown links
    link_pattern = re.compile(r"\[.*?\]\((.*?)\.md\)")

    # empty dictionary
    backlinks = defaultdict(set)
    filenames = []

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


def get_backlinks(notes_dir: str, note_relpath: str) -> str:
    # Get the graph
    d = build_backlinks(notes_dir)
    # Get the backlinks
    bl = {os.path.relpath(d, notes_dir) for d in d[note_relpath]}
    # Format the backlinks (TODO do a markdown link here)
    bl = {f"- [[{d}]]\n" for d in bl}
    bl = "\n\n ## Backlinks\n\n" + "".join(bl)
    return bl
