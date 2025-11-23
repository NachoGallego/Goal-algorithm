from fastapi import FastAPI
from .routers import results


def create_application() -> FastAPI:
    application = FastAPI(
        title="Local Football Data API",
        description="Serving historical football results and statistics.",
        version="1.0.0"
    )

    # --- ADD THIS ROOT ENDPOINT ---
    @application.get("/", tags=["Root"])
    def read_root():
        # This will respond to GET /
        return {
            "message": "Welcome to the GoalAlgo Data API.",
            "data_endpoint": "/results",
            
            "documentation": "/docs"
        }
    # ------------------------------

    # Register all modular routers here
    application.include_router(results.router)
    


    return application

# The 'app' object is the entry point for uvicorn
app = create_application()