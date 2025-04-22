
import operator
import logging
from protocol.protocol import Protocol

operators = {
    ">": operator.gt,
    "<": operator.lt,
    "eq": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne,
}


def _get_filter_args(filter_by):
    return filter_by.split(",")

def parse_filter_funct(data_to_filter, filter_by, file_name):

    args = _get_filter_args(filter_by)

    logging.info(f"Filter args: {args}")

    if file_name == "movies" and args[0] == "release_date":
        return filter_movies_by_date(data_to_filter, args)
    
    if file_name == "movies" and args[0] == "countries":
        return filter_movies_by_country(data_to_filter, args)

    if file_name == "movies" and args[0] == "single":
        return filter_movies_by_quantity(data_to_filter, args)

def filter_movies_by_date(data_to_filter, filter_args):
    result = []
    
    for data in data_to_filter.movies:
        movie_year = getattr(data, filter_args[0]).split("-")[0]
        op = filter_args[1]

        if op in operators and operators[op](int(movie_year), int(filter_args[2])):
            result.append(data)

    return result


def filter_movies_by_country(data_to_filter, filter_args):
    result = []
    
    for data in data_to_filter.movies:

        amount_to_check = int(filter_args[2])

        if len(data.countries) != amount_to_check:
            continue
        
        op = filter_args[1]
        countries_to_check = []

        for country in data.countries:
            if country.name not in countries_to_check:
                countries_to_check.append(country)

        if op in operators and operators[op](set(countries_to_check), set(filter_args[3:])):
            result.append(data)

    return result

def filter_movies_by_quantity(data_to_filter, filter_args):
    result = []
    
    for data in data_to_filter.movies:
        op = filter_args[1]

        if op in operators and operators[op](len(data.countries), int(filter_args[2])):
            result.append(data)
    
    return result