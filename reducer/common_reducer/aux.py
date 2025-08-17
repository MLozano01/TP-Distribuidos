import operator
import logging
from protocol.protocol import Protocol

def _get_filter_args(filter_by):
    return filter_by.split(",")

def parse_reduce_funct(data_to_reduce, reduce_by, result):
    protocol = Protocol()

    args = _get_filter_args(reduce_by)

    if args[0] == "top" and args[1] == "5":
        return reduce_top5(protocol.decode_aggr_batch(data_to_reduce), result)
    
    if args[0] == "top" and args[1] == "10":
        return reduce_top10(protocol.decode_actor_participations_batch(data_to_reduce), result)
    
    if args[0] == "max-min":
        return reduce_max_min(protocol.decode_aggr_batch(data_to_reduce), result)
    
    if args[0] == "avg":
        return reduce_avg(protocol.decode_aggr_batch(data_to_reduce), result)
    
    if args[0] == "q1":
        return reduce_query_one(protocol.decode_movies_msg(data_to_reduce), result)
    

def parse_final_result(reduce_by, partial_results):
    args = _get_filter_args(reduce_by)

    if args[0] == "avg":
        return calculate_avg(partial_results)
    
    if args[0] == "top":
        return calculate_top(partial_results, int(args[1]))
        
    if args[0] == "max-min":
        if not partial_results:
            return {}
        return calculate_max_min(partial_results)
    
    if args[0] == "q1":
        res = {}
        res["movies"] = partial_results
        return res

def reduce_top5(data_to_reduce, result):

    for data in data_to_reduce.aggr_row:
        result.setdefault(data.key, 0)
        result[data.key] += data.sum

    return result

def reduce_top10(data_to_reduce, result):

    for data in data_to_reduce.participations:
        result.setdefault(data.actor_name, 0)
        result[data.actor_name] += 1

    return result

def reduce_avg(data_to_reduce, result):

    sum_key = "sum"
    count_key = "count"

    for attribute in data_to_reduce.aggr_row:
        result.setdefault(attribute.key, {})
        result[attribute.key].setdefault(sum_key, 0)
        result[attribute.key].setdefault(count_key, 0)

        result[attribute.key][sum_key] += attribute.sum
        result[attribute.key][count_key] += attribute.count

    return result

def reduce_query_one(data_to_reduce, result):
    
    for data in data_to_reduce.movies:
        result.setdefault(data.title, [])
        genre_names = [genre.name for genre in data.genres if genre.name]
        result[data.title] = genre_names
        continue

    return result

def reduce_max_min(data_to_reduce, result):
    """Accumulates sum and count per movie so that the final average can be computed in parse_final_result."""

    for row in data_to_reduce.aggr_row:
        movie_dict = result.setdefault(row.key, {"sum": 0, "count": 0})
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

def calculate_max_min(partial_results):
    avg_per_movie = {}
    for title, sc in partial_results.items():
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

def calculate_top(partial_results, count):
    options = {
        5: "country",
        10: "actor", 
    }

    key = options[count]

    sorted_items = sorted(partial_results.items(), key=lambda kv: (-kv[1], kv[0]))
    top= dict(sorted_items[:count]) 
    logging.debug(f"[parse_final_result] top_{count}_{key}: {top}")
    
    res = {}
    res[key] = top
    logging.debug(f"[parse_final_result] Returning: {res}")

    return res