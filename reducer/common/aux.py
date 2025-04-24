
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

def parse_reduce_funct(data_to_reduce, reduce_by, result):

    protocol = Protocol()

    args = _get_filter_args(reduce_by)

    if args[0] == "top" and args[1] == "5":
        return reduce_top5(protocol.decode_aggr_batch(data_to_reduce), args, result)
    
    if args[0] == "top" and args[1] == "10":
        return reduce_top10(protocol.decode_actor_participations_batch(data_to_reduce), args, result)
    
    if args[0] == "max-min":
        return reduce_max_min(data_to_reduce, args, result)
    
    if args[0] == "avg":
        return reduce_avg(protocol.decode_aggr_batch(data_to_reduce), args, result)
    

def parse_final_result(reduce_by, partial_results):
    args = _get_filter_args(reduce_by)
    if args[0] == "avg":
        return calculate_avg(partial_results)
    if args[0] == "top":
        return partial_results


def reduce_top5(data_to_reduce, reduce_args, result):

    for data in data_to_reduce.aggr_row:

        if data.key not in result:
            result[data.key] = data.sum
            if len(result) > int(reduce_args[1]):
                last_country = min(result, key=result.get)
                result.pop(last_country)

        else:
            if result[data.key] < data.sum:
                result[data.key] = data.sum

    return result

def reduce_top10(data_to_reduce, reduce_args, result):

    logging.info(f"Data to reduce: {data_to_reduce}")

    for data in data_to_reduce.participations:
        if data.actor_name in result:
            result[data.actor_name] += 1
        else:
            result[data.actor_name] = 1
            if len(result) > int(reduce_args[1]):
                last_country = min(result, key=result.get)
                result.pop(last_country)

    return result

def reduce_avg(data_to_reduce, reduce_args, result):

    for attribute in data_to_reduce.aggr_row:
        result.setdefault(attribute.key, {})
        result[attribute.key].setdefault("sum", 0)
        result[attribute.key].setdefault("count", 0)

        result[attribute.key]["sum"] += attribute.sum
        result[attribute.key]["count"] += attribute.count

    return result

def reduce_max_min(data_to_reduce, reduce_args, result):

    for data in data_to_reduce.movies:
        continue

    return result

def calculate_avg(partial_result):
    result = {}

    for key, value in partial_result.items():
        result.setdefault(key, 0)
        if value["count"] > 0:
            result[key] = value["sum"] / value["count"]
        else:
            result[key]= 0
    
    return result

def reduce_max_min(data_to_reduce, filter_by, result):
    pass