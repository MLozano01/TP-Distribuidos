
import operator

operators = {
    ">": operator.gt,
    "<": operator.lt,
    "=": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne,
}

def parse_filter_funct(data_to_filter, filter_by, file_name):

    if file_name == "movies" and filter_by.split("_")[0] == "releaseDate":
        return filter_movies_by_date(data_to_filter, filter_by, file_name)

def filter_movies_by_date(data_to_filter, filter_by, file_name):
    result = {}
    
    for data in data_to_filter[file_name]:
        filter_info = filter_by.split("_")
        movie_year = data[filter_info[0]].split("-")[0]
        op = filter_info[1]

        if op in operators:
            if operators[op](int(movie_year), int(filter_info[2])):
                result[data['title']] = data

    return result
