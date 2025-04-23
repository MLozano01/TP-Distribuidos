
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

    args = _get_filter_args(reduce_by)

    if args[0] == "top":
        return reduce_top(data_to_reduce, args, result)
    
    if args[0] == "max-min":
        return reduce_max_min(data_to_reduce, args, result)
    
    if args[0] == "avg":
        return reduce_avg(data_to_reduce, args, result)
    

def parse_final_result(reduce_by, partial_results):
    args = _get_filter_args(reduce_by)
    if args[0] == "avg":
        return calculate_avg(partial_results)


def reduce_top(data_to_reduce, reduce_args, result):

    logging.info(f"DATA TO REDUCE {data_to_reduce}" )

    result.setdefault(reduce_args[0], [])   

    logging.info(f"Result: {result}")
    logging.info(f"Reduce args: {result[reduce_args[0]]}")
    
    for data in data_to_reduce.aggr_row:
        logging.info(f"Data: {data}")

        if result[reduce_args[0]] == [] or len(result[reduce_args[0]]) < int(reduce_args[1]):
            result[reduce_args[0]].append(data)
            continue
        if result[reduce_args[0]][-1].sum < data.sum:
            if len(result[reduce_args[0]]) == int(reduce_args[1]):
                result[reduce_args[0]][-1] = data
            result.append(data)

    result[reduce_args[0]].sort(key=lambda x: getattr(x, reduce_args[0]), reverse=True)

    return result

def reduce_avg(data_to_reduce, reduce_args, result):

    for attribute in data_to_reduce.aggr_row:
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

def reduce_max_min(data_to_reduce, filter_by, result):
    pass