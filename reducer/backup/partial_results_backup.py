import json
import os
import logging
from pathlib import Path



def make_new_backup(info_to_save, node_file):
    tempfile = f"/backup/temp_{node_file}"
    full_node_file = f"/backup/{node_file}"
    try:
        with open(tempfile, 'w') as f:
            json.dump(info_to_save, f)
            f.flush()

        os.replace(tempfile, full_node_file)

    except Exception as e:
        logging.error(f"ERROR saving info to file: {e}")

def from_backup(node_file):
    backup = {}
    backup_file = f"/backup/{node_file}"

    if not Path(backup_file).exists():
        return {}

    try:
        with open(backup_file, 'r') as f:
            backup = json.load(f)
    except json.JSONDecodeError as e:
        logging.error("There was an error: {e}")
        backup = {}
    return backup