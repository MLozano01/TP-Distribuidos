from logic.ratings_join_strategy import RatingsJoinStrategy
from logic.credits_join_strategy import CreditsJoinStrategy

def get_join_strategy(strategy_name: str):
    """
    Factory function to get a join strategy instance.
    """
    if strategy_name == "RATINGS":
        return RatingsJoinStrategy()
    elif strategy_name == "CREDITS":
        return CreditsJoinStrategy()
    else:
        raise ValueError(f"Unknown join strategy: {strategy_name}") 