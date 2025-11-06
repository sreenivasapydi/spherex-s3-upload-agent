import uvicorn
from app.config import settings

if __name__ == "__main__":
    uvicorn.run("app.main:app", 
                host="0.0.0.0", 
                port=settings.APP_PORT,
                reload_dirs=["app"],
                reload=settings.APP_ENV == "development")

