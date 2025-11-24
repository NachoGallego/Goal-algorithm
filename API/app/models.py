from pydantic import BaseModel
from typing import Optional, Union

# Define the data structure for a single football result (match)
class FootballResult(BaseModel):
    # Team names
    home_team: str
    away_team: str
    
    # Probability/Statistical data
    p_home_0: float
    p_home_1: float
    p_home_2: float
    p_away_0: float
    p_away_1: float
    p_away_2: float
    
    # Final Scores (Scores are forced to be integers)
    pred_1: int
    pred_2: int
    
    # Optional fields (can be null/NaN in the data)
    # Union handles cases where the data might be an integer, float, or string
    result_text: Optional[Union[float, str, int]] = None
    bet_value: Optional[Union[float, str, int]] = None