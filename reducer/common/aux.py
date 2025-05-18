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

def parse_reduce_funct(data_to_reduce, reduce_by, result, client_id):
    protocol = Protocol()

    args = _get_filter_args(reduce_by)

    if args[0] == "top" and args[1] == "5":
        return reduce_top5(protocol.decode_aggr_batch(data_to_reduce), args, result, client_id)
    
    if args[0] == "top" and args[1] == "10":
        logging.info(f"[parse_reduce_funct] client_id: {client_id}")
        return reduce_top10(protocol.decode_actor_participations_batch(data_to_reduce), args, result, client_id)
    
    if args[0] == "max-min":
        return reduce_max_min(protocol.decode_movies_msg(data_to_reduce), args, result, client_id)
    
    if args[0] == "avg":
        return reduce_avg(protocol.decode_aggr_batch(data_to_reduce), args, result, client_id)
    
    if args[0] == "q1":
        return reduce_query_one(protocol.decode_movies_msg(data_to_reduce), args, result, client_id)
    

def parse_final_result(reduce_by, partial_results, client_id):
    args = _get_filter_args(reduce_by)

    client_partial_results = partial_results.get(client_id, {})

    if args[0] == "avg":
        return calculate_avg(client_partial_results)
    if args[0] == "top" and args[1] == "5":
        # Sort countries by sum (value) in descending order

        sorted_countries = sorted(client_partial_results.items(), key=operator.itemgetter(1), reverse=True)
        # Take the top 5
        top_5_countries = dict(sorted_countries[:int(args[1])])
        logging.debug(f"[parse_final_result] top_5_countries: {top_5_countries}") # Log calculated top 5

        res = {}
        res["country"] = top_5_countries # Assign the actual top 5 dictionary
        logging.debug(f"[parse_final_result] Returning: {res}") # Log return value
        return res
    if args[0] == "top" and args[1] == "10":
        # Sort actors by count (value) in descending order
        sorted_actors = sorted(client_partial_results.items(), key=operator.itemgetter(1), reverse=True)
        # Take the top 10
        top_10_actors = dict(sorted_actors[:int(args[1])]) 
        logging.debug(f"[parse_final_result] top_10_actors: {top_10_actors}")
        
        res = {}
        res["actor"] = top_10_actors # Assign the actual top 10 dictionary
        logging.debug(f"[parse_final_result] Returning: {res}")
        return res
    if args[0] == "max-min":
        res = {}
        res["max-min"] = client_partial_results
        return res
    
    if args[0] == "q1":
        res = {}
        res["movies"] = client_partial_results
        return res

def reduce_top5(data_to_reduce, reduce_args, result, client_id):

    result.setdefault(client_id, {})

    for data in data_to_reduce.aggr_row:
        result[client_id].setdefault(data.key, 0)
        result[client_id][data.key] += data.sum

    return result

def reduce_top10(data_to_reduce, reduce_args, result, client_id):

    result.setdefault(client_id, {})

    for data in data_to_reduce.participations:
        result[client_id].setdefault(data.actor_name, 0)
        result[client_id][data.actor_name] += 1

    return result

def reduce_avg(data_to_reduce, reduce_args, result, client_id):
    result.setdefault(client_id, {})

    sum_key = "sum"
    count_key = "count"

    for attribute in data_to_reduce.aggr_row:
        result[client_id].setdefault(attribute.key, {})
        result[client_id][attribute.key].setdefault(sum_key, 0)
        result[client_id][attribute.key].setdefault(count_key, 0)

        result[client_id][attribute.key][sum_key] += attribute.sum
        result[client_id][attribute.key][count_key] += attribute.count

    return result

def reduce_query_one(data_to_reduce, reduce_args, result, client_id):
    
    result.setdefault(client_id, {})

    for data in data_to_reduce.movies:
        result[client_id].setdefault(data.title, [])
        genre_names = [genre.name for genre in data.genres if genre.name]
        result[client_id][data.title] = genre_names
        continue

    return result

def reduce_max_min(data_to_reduce, reduce_args, result, client_id):
    result.setdefault(client_id, {})

    for data in data_to_reduce.movies:
        client_result = result[client_id]
        if len(client_result) < 2:
            client_result[data.title] = data.average_rating
        else: 
            max_rating = max(client_result, key=client_result.get)
            min_rating = min(client_result, key=client_result.get)

            if client_result[max_rating] < data.average_rating:
                client_result.pop(max_rating)
                client_result[data.title] = data.average_rating
            elif client_result[min_rating] > data.average_rating:
                client_result.pop(min_rating)
                client_result[data.title] = data.average_rating

    logging.info(f"[REDUCE_MAX_MIN] client_id: {client_id}, result: {result}")

    return result

def calculate_avg(partial_result):
    result = {}
    res = {}
    logging.info(f"partial_result: {partial_result}")
    for key, value in partial_result.items():
        result.setdefault(key, 0)
        if value["count"] > 0:
            result[key] = value["sum"] / value["count"]
        else:
            result[key]= 0

    res["sentiment"] = result    
    return res