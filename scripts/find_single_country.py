#!/usr/bin/env python3
import csv
import ast
from pathlib import Path

# --- Ajuste de paths relativo a la ubicación del script ---
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
DATA_DIR   = ROOT_DIR / 'data'
MOVIES_CSV = DATA_DIR / 'movies_metadata.csv'
# ---------------------------------------------------------

def find_first_n_by_country(country_code, n=2):
    """
    Devuelve una lista de tuplas (title, budget) para las primeras n películas
    que tengan exactamente un production_country == country_code y budget > 0.
    """
    results = []
    with MOVIES_CSV.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parsear production_countries
            raw = row.get('production_countries') or '[]'
            try:
                countries = ast.literal_eval(raw)
            except Exception:
                continue

            # Filtrar: un solo país, que sea el deseado
            if not (isinstance(countries, list) and len(countries) == 1):
                continue
            iso = countries[0].get('iso_3166_1', '').upper()
            if iso != country_code.upper():
                continue

            # Budget > 0
            try:
                budget = float(row.get('budget') or 0)
            except ValueError:
                continue
            if budget <= 0:
                continue

            # Recoger título y presupuesto
            title = (row.get('title') or '').strip()
            if title:
                results.append((title, budget))
                if len(results) >= n:
                    break

    return results

def main():
    # Buscar los 2 primeros de Argentina y de Italy
    ar_movies = find_first_n_by_country('AR', n=2)
    it_movies = find_first_n_by_country('IT', n=2)

    # Mostrar resultados
    total_ar = sum(b for _, b in ar_movies)
    total_it = sum(b for _, b in it_movies)

    print("Películas (solo AR, budget>0):")
    for title, budget in ar_movies:
        print(f" - {title}: ${budget:,.2f}")
    print(f"Total budget Argentina: ${total_ar:,.2f}\n")

    print("Películas (solo IT, budget>0):")
    for title, budget in it_movies:
        print(f" - {title}: ${budget:,.2f}")
    print(f"Total budget Italy:     ${total_it:,.2f}")

if __name__ == '__main__':
    main()
