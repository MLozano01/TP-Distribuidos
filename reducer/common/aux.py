
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

def parse_reduce_funct(data_to_filter, filter_by, result):

    args = _get_filter_args(filter_by)

    if args[0] == "top":
        return reduce_top(data_to_filter, args, result)
    
    if args[0] == "max-min":
        return reduce_max_min(data_to_filter, args, result)
    
    if args[0] == "avg":
        return reduce_avg(data_to_filter, args, result)
    
    if args[0] == "calc_avg":
        return calculate_avg(result)



def reduce_top(data_to_filter, reduce_args, result):
    
    #TODO: Change the loop depending on the way data comes in

    for data in data_to_filter.movies:
        if result == [] or len(result) < int(reduce_args[1]):
            result.append(data)
            continue
        if result[-1] < data.reduce_args[2]:
            if len(result) < int(reduce_args[1]):
                result.append(data)
            else:
                result[-1] = data

    result.sort(key=lambda x: getattr(x, reduce_args[0]), reverse=True)

    return result

def reduce_avg(data_to_filter, reduce_args, result):


    for attribute in data_to_filter.aggr_row:
        result.setdefault(attribute.key, {})
        result[attribute.key].setdefault("sum", 0)
        result[attribute.key].setdefault("count", 0)

        result[attribute.key]["sum"] += attribute.sum
        result[attribute.key]["count"] += attribute.count

    # logging.info(f"Result: {result}")

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

def reduce_max_min(data_to_filter, filter_by, result):
    pass