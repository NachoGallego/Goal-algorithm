from fastapi import APIRouter, HTTPException
from ..models import FootballResult # Relative import of your Pydantic model
from ..data_loader import results_data # Import the loaded data

# Create a new router object. The 'prefix' ensures all routes start with /results
router = APIRouter(
    prefix="/results", 
    tags=["Results"] 
)

# --- Standard API Calls ---

@router.get("/", response_model=list[FootballResult], summary="Get All Football Results")
def get_all_results():
    """
    Returns the complete list of all match results and statistics.
    """
    return results_data

@router.get("/{row_number}", response_model=FootballResult, summary="Get Single Result by Row Number")
def get_single_result(row_number: int):
    """
    Returns a single football result based on its 1-based row number/ID.
    """
    index = row_number - 1
    
    if 0 <= index < len(results_data):
        return results_data[index]
    
    # Standard REST API status code for a resource not found
    raise HTTPException(status_code=404, detail="Result not found. Row number out of range.")