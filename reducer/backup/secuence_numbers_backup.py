import json
import os
import logging
from pathlib import Path
import subprocess



def make_new_secuence_number_backup(info_to_save, node_file):
    tempfile = f"/backup/temp_{node_file}"
    full_node_file = f"/backup/{node_file}"
    try:
        with open(tempfile, 'a') as f:
            f.wite(info_to_save)
            f.flush()

        os.replace(tempfile, full_node_file)

    except Exception as e:
        logging.error(f"ERROR saving secuence numbe info to file: {e}")


def load_saved_data(backup_file):
    if not Path(backup_file).exists():
        return {}

    result = subprocess.run(['tail', '-n', '1', backup_file], capture_output=True, text=True)
    print(result.stdout)