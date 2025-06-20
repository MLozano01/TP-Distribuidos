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
        return reduce_top10(protocol.decode_actor_participations_batch(data_to_reduce), args, result, client_id)
    
    if args[0] == "max-min":
        return reduce_max_min(protocol.decode_aggr_batch(data_to_reduce), args, result, client_id)
    
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


        sorted_countries = sorted(client_partial_results.items(), key=operator.itemgetter(1), reverse=True)
        top_5_countries = dict(sorted_countries[:int(args[1])])
        logging.debug(f"[parse_final_result] top_5_countries: {top_5_countries}")

        res = {}
        res["country"] = top_5_countries 
        logging.debug(f"[parse_final_result] Returning: {res}") 
        return res
    if args[0] == "top" and args[1] == "10":
        sorted_actors = sorted(client_partial_results.items(), key=operator.itemgetter(1), reverse=True)
        top_10_actors = dict(sorted_actors[:int(args[1])]) 
        logging.debug(f"[parse_final_result] top_10_actors: {top_10_actors}")
        
        res = {}
        res["actor"] = top_10_actors
        logging.debug(f"[parse_final_result] Returning: {res}")
        return res
    if args[0] == "max-min":
        if not client_partial_results:
            return {}

        avg_per_movie = {}
        for title, sc in client_partial_results.items():
            total_sum = sc.get("sum", 0)
            total_count = sc.get("count", 0)
            if total_count > 0:
                avg_per_movie[title] = total_sum / total_count

        if not avg_per_movie:
            return {}

        max_title = max(avg_per_movie, key=avg_per_movie.get)
        min_title = min(avg_per_movie, key=avg_per_movie.get)

        res = {
            "max-min": {
                max_title: avg_per_movie[max_title],
                min_title: avg_per_movie[min_title],
            }
        }
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
    """Accumulates sum and count per movie so that the final average can be computed in parse_final_result."""
    result.setdefault(client_id, {})

    for row in data_to_reduce.aggr_row:
        movie_dict = result[client_id].setdefault(row.key, {"sum": 0, "count": 0})
        movie_dict["sum"] += row.sum
        movie_dict["count"] += row.count

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