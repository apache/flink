#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
"""Fix LogicalProject format in .out files."""

import re
import sys
import os

def convert_logical_project(match):
    """Convert LogicalProject(name=[$0], name2=[$1]) to LogicalProject(select=[name, name2])"""
    inner = match.group(1)
    fields = []
    pattern = r'(\w+)=\[([^\]]+)\]'
    for field_match in re.finditer(pattern, inner):
        field_name = field_match.group(1)
        expr = field_match.group(2)
        if re.match(r'^\$\d+$', expr):
            fields.append(field_name)
        else:
            fields.append(f'{expr} AS {field_name}')
    return 'LogicalProject(select=[' + ', '.join(fields) + '])'

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    if 'LogicalProject(' not in content:
        return False
    pattern = r'LogicalProject\(([^)]+)\)'
    new_content = re.sub(pattern, convert_logical_project, content)
    if new_content != content:
        with open(filepath, 'w') as f:
            f.write(new_content)
        return True
    return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python fix_out_files.py <directory>")
        sys.exit(1)
    directory = sys.argv[1]
    count = 0
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.out'):
                filepath = os.path.join(root, filename)
                if process_file(filepath):
                    print(f"Fixed: {filepath}")
                    count += 1
    print(f"Total fixed: {count} files")

if __name__ == '__main__':
    main()
