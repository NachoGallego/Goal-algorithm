from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from typing import Optional, List
from pathlib import Path
# Assuming models.py is at the same level as app/ and routers/ (i.e., in the project root's parent)
from ..models import FootballResult
# Assuming data_loader is in the 'app' folder, making this import relative to 'routers' folder's parent
from ..data_loader import results_data
# Create a new router object.
router = APIRouter(
    prefix="/results", 
    tags=["Results"] 
)

# --- JINJA2 SETUP within the Router File ---
# We need to calculate the path to the 'templates' folder from the router's location.
# This assumes the project root structure: [root]/routers/results.py and [root]/templates/
BASE_DIR = Path(__file__).resolve().parent.parent # Navigate up 3 levels to the root
TEMPLATES_DIR = BASE_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
# --------------------------------------------


# --- 1. JSON API Endpoint (Existing) ---
@router.get("/", response_model=List[FootballResult], summary="Get Football Results (Filtered)")
def get_all_results(
    has_bets: Optional[bool] = None 
):
    """
    Returns the complete list of all match results (JSON).
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


# --- 2. HTML TEMPLATE Endpoint (NEW) ---
@router.get("/summary", summary="Get Results Summary (HTML)")
async def get_results_summary(request: Request):
    """
    Returns a summary dashboard of the football results data, rendered as HTML.
    """
    total_matches = len(results_data)
    matches_with_bets = sum(1 for r in results_data if r.bet_value is not None)
    
    context = {
        "request": request, # MANDATORY
        "title": "Football Results Summary",
        "total_matches": total_matches,
        "matches_with_bets": matches_with_bets,
        "results": results_data  # Pass all data for rendering the table
    }
    
    # Renders the template located in the 'templates' folder
    return templates.TemplateResponse(
        "results_summary.html", 
        context
    )
# ----------------------------------------


# --- 3. JSON API Endpoint (Existing) ---
@router.get("/{row_number}", response_model=FootballResult, summary="Get Single Result by Row Number")
def get_single_result(row_number: int):
    """
    Returns a single football result based on its 1-based row number/ID (JSON).
    """
    index = row_number - 1
    
    if 0 <= index < len(results_data):
        return results_data[index]
    
    raise HTTPException(status_code=404, detail="Result not found. Row number out of range.")