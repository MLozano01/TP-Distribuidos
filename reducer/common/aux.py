
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

    # {'POSITIVE': {'sum': 4.336928069591522, 'count': 2}}

    pass


def reduce_max_min(data_to_filter, filter_by, result):
    pass