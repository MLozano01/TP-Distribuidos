#!/bin/bash
readarray -t BACKUP_FOLDERS < <(find "$PWD" -maxdepth 5 -type d -name "backup")

# Check if any "backup" folders were found
if [ -z "$BACKUP_FOLDERS" ]; then
  echo "No directories named 'backup' found within '$PWD' up to 5 levels deep."
  exit 0
fi

echo "$BACKUP_FOLDERS"

for FOLDER_TO_CLEAN in "${BACKUP_FOLDERS[@]}"; do
  echo "" # Newline for readability
  echo "Processing folder: -> $FOLDER_TO_CLEAN"
  rm -rf "$FOLDER_TO_CLEAN/"*
done

echo ""
echo "--- All 'backup' folders processed. ---"

exit 0
