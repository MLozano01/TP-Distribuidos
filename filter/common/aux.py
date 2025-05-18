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

def parse_filter_funct(data_to_filter, filter_by):

    args = _get_filter_args(filter_by)

    if args[0] == "release_date":
        return filter_movies_by_date(data_to_filter, args)
    
    if args[0] == "countries":
        return filter_movies_by_country(data_to_filter, args)

    if args[0] == "single":
        return filter_movies_by_quantity(data_to_filter, args)
    
    if args[0] == "decade":
        return filter_movies_by_dacade(data_to_filter, args)


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

        countries_to_check = []
        for country in data.countries:
            if country.name not in countries_to_check:
                countries_to_check.append(country.name)

        if set(filter_args[3:]).issubset(set(countries_to_check)):
            result.append(data)            

    return result

def filter_movies_by_quantity(data_to_filter, filter_args):
    result = []
    
    for data in data_to_filter.movies:
        op = filter_args[1]

        if op in operators and operators[op](len(data.countries), int(filter_args[2])):
            result.append(data)
    
    return result

def filter_movies_by_dacade(data_to_filter, filter_args):
    result = []
    
    for data in data_to_filter.movies:
        movie_year = getattr(data, "release_date").split("-")[0]
        op = filter_args[1]

        if int(movie_year) >= int(filter_args[2]) and int(movie_year) < (int(filter_args[2]) + int(filter_args[1])):
            result.append(data)

    return result

