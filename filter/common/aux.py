
import operator
import logging

operators = {
    ">": operator.gt,
    "<": operator.lt,
    "eq": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne,
}

def parse_filter_funct(data_to_filter, filter_by, file_name):

    if file_name == "movies" and filter_by.split("_")[0] == "releaseDate":
        return filter_movies_by_date(data_to_filter, filter_by, file_name)
    
    if file_name == "movies" and filter_by.split("_")[0] == "countries":
        return filter_movies_by_country(data_to_filter, filter_by, file_name)


def filter_movies_by_date(data_to_filter, filter_by, file_name):
    result = {}
    
    for data in data_to_filter[file_name]:
        filter_info = filter_by.split("_")
        movie_year = data[filter_info[0]].split("-")[0]
        op = filter_info[1]

        if op in operators and operators[op](int(movie_year), int(filter_info[2])):
            result[data['title']] = data

    return result


def filter_movies_by_country(data_to_filter, filter_by, file_name):
    result = {}
    
    for data in data_to_filter[file_name]:
        filter_info = filter_by.split("_")

        amount_to_check = int(filter_info[3])

        if len(data[filter_info[0]]) != amount_to_check:
            continue
        
        op = filter_info[2]
        countries_to_check = []

        for i in range(amount_to_check):
            country = data[filter_info[0]][i][filter_info[1]]
            if country not in countries_to_check:
                countries_to_check.append(country)

        if op in operators and operators[op](set(countries_to_check), set(filter_info[4:])):
            result[data['title']] = data

    return result