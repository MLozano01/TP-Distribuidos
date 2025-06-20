import os
import logging
from pathlib import Path
import glob

BASE_FILE_NAME = "secuence_numbers_client"

def make_new_secuence_number_backup(info_to_save, client_id):
    tempfile = f"/backup/temp_{BASE_FILE_NAME}_{client_id}"
    full_node_file = f"/backup/{BASE_FILE_NAME}_{client_id}"
    try:
        with open(tempfile, 'a') as f:
            f.wite(f"{info_to_save}\n")
            f.flush()
        os.replace(tempfile, full_node_file)

    except Exception as e:
        logging.error(f"ERROR saving secuence numbe info to file {full_node_file}: {e}")


def load_saved_data():
    try:
        backup = {}
        for filepath in glob.glob(f'/backup/{BASE_FILE_NAME}_*'):
            client = filepath.split("_")[3]
            with open(filepath) as f:
                backup.setdefault(client, [])
                for line in f:
                    pass            
        
    except Exception as e:
        logging.error(f"ERROR reading from file: {e}")