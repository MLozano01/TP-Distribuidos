from logic.ratings_join_strategy import RatingsJoinStrategy
from logic.credits_join_strategy import CreditsJoinStrategy

def get_join_strategy(strategy_name: str, replica_id: int, replicas_count: int):
    """
    Factory function to get a join strategy instance.
    """
    if strategy_name == "RATINGS":
        return RatingsJoinStrategy(replica_id, replicas_count)
    elif strategy_name == "CREDITS":
        return CreditsJoinStrategy(replica_id, replicas_count)
    else:
        raise ValueError(f"Unknown join strategy: {strategy_name}") 