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

def parse_filter_funct(data_to_filter, filter_by, file_name):

    if file_name == "movies" and filter_by.split(",")[0] == "release_date":
        return filter_movies_by_date(data_to_filter, filter_by, file_name)
    
    if file_name == "movies" and filter_by.split(",")[0] == "countries":
        return filter_movies_by_country(data_to_filter, filter_by, file_name)


def filter_movies_by_date(data_to_filter, filter_by, file_name):
    result = []
    
    for data in data_to_filter.movies:
        filter_info = filter_by.split(",")
        movie_year = getattr(data, filter_info[0]).split("-")[0]
        op = filter_info[1]

        if op in operators and operators[op](int(movie_year), int(filter_info[2])):
            result.append(data)

    return result


def filter_movies_by_country(data_to_filter, filter_by, file_name):
    result = []
    try:
        # Format: countries,name,eq,1,Argentina (index 3 is potentially legacy)
        filter_info = filter_by.split(",")
        # We assume the 'eq' operator here implies checking for the *presence* of the country.
        target_countries = set(filter_info[4:]) # Get target country NAMES as a set
        if not target_countries:
            logging.warning(f"No target countries found in filter string: {filter_by}")
            return []
        # logging.debug(f"Applying country filter: TargetCountries={target_countries}")
    except IndexError as e:
        logging.error(f"Error parsing country filter string '{filter_by}': {e}")
        return []
    
    for data in data_to_filter.movies:
        try:
            # Get the set of NAMES of the countries for this movie
            movie_countries = {country.name for country in data.countries if country.name}
            # logging.debug(f"Checking Movie ID={data.id}, Title='{data.title}', Countries={movie_countries}")

            # Check if ANY of the target country NAMES exist in the movie's country NAMES
            intersection = movie_countries.intersection(target_countries)
            match = bool(intersection) # True if there is at least one common country name
            
            # logging.debug(f"--> Country Condition ('{target_countries}' in {movie_countries}) is {match}")
            if match:
                result.append(data)
                
        except Exception as e:
             logging.error(f"Error filtering movie ID {data.id} by country: {e}", exc_info=False)

    return result