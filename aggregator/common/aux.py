

def parse_aggregate_func(data_to_filter, key, field, operations, file_name):
    if file_name == "movies" and key == "countries":
        return aggr_movies_by_country(data_to_filter, field, operations)
    
    result = dict()
    for data in getattr(data_to_filter, file_name):
        update_results(data, key, field, operations, result)
    return result

def aggr_movies_by_country(data_to_filter, field, operations):
    result = dict()
    for movie in data_to_filter.movies:
        # should be only 1 country
        for country in movie.countries:
            movie_key = country.name
            operations_partial = result.get(movie_key, dict())
            apply_operations(operations_partial, movie, field, operations)
    return result

def update_results(data_row, key, field, operations, result):
    data_key = getattr(data_row, key)
    operations_partial = result.get(data_key, dict())
    apply_operations(operations_partial, data_row, field, operations)

def apply_operations(operations_partial, row_data, field, operations):
    if "sum" in operations:
        operations_partial["sum"] += getattr(row_data, field)
    if "count" in operations:
        operations_partial["count"] += 1