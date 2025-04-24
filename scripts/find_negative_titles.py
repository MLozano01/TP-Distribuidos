#!/usr/bin/env python3
import csv
import argparse
from pathlib import Path
from transformers import pipeline

# Ruta fija al CSV de movies
BASE_DIR     = Path(__file__).resolve().parent.parent
MOVIES_CSV   = BASE_DIR / 'data' / 'movies_metadata.csv'

def find_negative_titles(max_negative=3):
    # Inicializar pipeline de sentimiento
    sentiment_analyzer = pipeline(
        'sentiment-analysis',
        model='distilbert-base-uncased-finetuned-sst-2-english',
    )

    negative_titles = []
    with MOVIES_CSV.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            overview = (row.get('overview') or '').strip()
            if not overview or overview.lower() in {'na', 'none'}:
                continue

            result = sentiment_analyzer(overview, truncation=True)
            if result[0]['label'] == 'NEGATIVE':
                title = (row.get('title') or '').strip()
                if title:
                    negative_titles.append(title)
                    if len(negative_titles) >= max_negative:
                        break

    return negative_titles

def main():
    parser = argparse.ArgumentParser(
        description="Devuelve los títulos de las primeras N películas con sentimiento negativo."
    )
    parser.add_argument(
        "-n", "--number",
        type=int,
        default=3,
        help="Cuántos títulos negativos imprimir (por defecto: 3)"
    )
    args = parser.parse_args()

    titles = find_negative_titles(max_negative=args.number)
    print("\nPelículas con overview negativo:")
    for t in titles:
        print(f" - {t}")

if __name__ == '__main__':
    main()
