import os

import uvicorn

from app.config import settings
from app.main import app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", str(settings.port)))
    uvicorn.run(app, host=settings.host, port=port)
