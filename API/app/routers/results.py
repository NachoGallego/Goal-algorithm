from fastapi import APIRouter, HTTPException
from typing import Optional, List # Import Optional and List for type hinting
from ..models import FootballResult
from ..data_loader import results_data

# Create a new router object.
router = APIRouter(
    prefix="/results", 
    tags=["Results"] 
)



@router.get("/", response_model=List[FootballResult], summary="Get Football Results (Filtered)")
def get_all_results(
    has_bets: Optional[bool] = None 
):
    """
    Returns the complete list of all match results.

    You can filter the results to only include games where a bet has been placed
    by setting the 'has_bets' query parameter to 'true'.
    """
    filtered_results = results_data 
    if has_bets is not None:
      
        if has_bets is True:
            
            filtered_results = [r for r in results_data if r.bet_value is not None]
        
        elif has_bets is False:
    
            filtered_results = [r for r in results_data if r.bet_value is None]
    
    return filtered_results



@router.get("/{row_number}", response_model=FootballResult, summary="Get Single Result by Row Number")
def get_single_result(row_number: int):
    """
    Returns a single football result based on its 1-based row number/ID.
    """
    index = row_number - 1
    
    if 0 <= index < len(results_data):
        return results_data[index]
    
    raise HTTPException(status_code=404, detail="Result not found. Row number out of range.")