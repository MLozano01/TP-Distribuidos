class RequeueException(Exception):
    """Signal that the current message should be nacked / re-queued."""
    pass 