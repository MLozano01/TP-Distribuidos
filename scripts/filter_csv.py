#!/usr/bin/env python3
import csv
from pathlib import Path

# --- Ajuste de paths relativo a la ubicación del script ---
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
DATA_DIR   = ROOT_DIR / 'data'
# ---------------------------------------------------------

MOVIES_IN     = DATA_DIR / 'movies_metadata.csv'
RATINGS_IN    = DATA_DIR / 'ratings.csv'
CREDITS_IN    = DATA_DIR / 'credits.csv'

MOVIES_OUT    = DATA_DIR / 'movies_metadata_filtered.csv'
RATINGS_OUT   = DATA_DIR / 'ratings_filtered.csv'
CREDITS_OUT   = DATA_DIR / 'credits_filtered.csv'

TITLES_TO_KEEP = {
    "La Cienaga",
    "Burnt Money",
    "The City of No Limits",
    "Nicotina",
    "Lost Embrace",
    "Whisky",
    "The Holy Girl",
    "The Aura",
    "Bombón: The Dog",
    "Rolling Family",
    "The Method",
    "Every Stewardess Goes to Heaven",
    "Tetro",
    "The Secret in Their Eyes",
    "Liverpool",
    "The Headless Woman",
    "The Last Summer of La Boyita",
    "The Appeared",
    "The Fish Child",
    "Cleopatra",
    "Roma",
    "Conversations with Mother",
    "The Good Life",
    "The forbidden education",
    "Left for Dead",
    "Jumanji",
    "Grumpier Old Men",
    "Father of the Bride Part II"
}

def filter_movies_and_collect_ids():
    movie_ids = set()
    with MOVIES_IN.open(newline='', encoding='utf-8') as infile, \
         MOVIES_OUT.open('w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            title = (row.get('title') or '').strip()
            if title in TITLES_TO_KEEP:
                writer.writerow(row)
                movie_id = (row.get('id') or '').strip()
                if movie_id:
                    movie_ids.add(movie_id)

    print(f"📝 Películas filtradas escritas en: {MOVIES_OUT}")
    return movie_ids

def filter_ratings_by_movie_ids(movie_ids):
    with RATINGS_IN.open(newline='', encoding='utf-8') as infile, \
         RATINGS_OUT.open('w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            movie_id = (row.get('movieId') or '').strip()
            if movie_id in movie_ids:
                writer.writerow(row)

    print(f"📝 Ratings filtradas escritas en: {RATINGS_OUT}")

def filter_credits_by_movie_ids(movie_ids):
    with CREDITS_IN.open(newline='', encoding='utf-8') as infile, \
         CREDITS_OUT.open('w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            credit_id = (row.get('id') or '').strip()
            if credit_id in movie_ids:
                writer.writerow(row)

    print(f"📝 Credits filtradas escritas en: {CREDITS_OUT}")

def main():
    # 1) Filtrar movies_metadata y recoger sus IDs
    movie_ids = filter_movies_and_collect_ids()
    # 2) Filtrar ratings.csv con esos IDs
    filter_ratings_by_movie_ids(movie_ids)
    # 3) Filtrar credits.csv con esos mismos IDs
    filter_credits_by_movie_ids(movie_ids)

if __name__ == '__main__':
    main()
