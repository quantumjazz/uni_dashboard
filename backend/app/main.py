from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request
from fastapi.staticfiles import StaticFiles

from backend.app.api.routes import router
from backend.app.cache.database import init_db
from backend.app.config import get_settings


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


settings = get_settings()
app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)


@app.middleware("http")
async def disable_frontend_asset_caching(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.endswith((".js", ".css", ".html")):
        response.headers["Cache-Control"] = "no-store"
    return response


app.include_router(router)
app.mount("/", StaticFiles(directory=settings.frontend_dir, html=True), name="frontend")
