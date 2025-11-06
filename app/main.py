import fastapi
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from app.health.health_routes import router as health_router
from app.config import settings

router = APIRouter()


def create_app() -> FastAPI:
    app = fastapi.FastAPI(
        title=settings.SERVICE_NAME,
        description=settings.SERVICE_DESCRIPTION,
        version=settings.SERVICE_VERSION,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["Location"],
    )
    app.include_router(health_router)

    return app

app = create_app()

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Re-raise HTTPExceptions so their status codes/details are preserved."""
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler that returns 500 for unexpected errors."""
    # logger.exception(f"Unhandled exception: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
