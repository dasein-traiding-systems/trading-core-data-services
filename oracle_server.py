from patch_submod import dummy  # <- REQUIRED
import services.backend.server
from services.backend import app
from tc.core.utils.logs import setup_logger
from tc.config import Config
dummy()
config = Config.load_from_env()

if __name__ == "__main__":
    # import uvicorn
    # uvicorn.run(app, host="0.0.0.0", port=8777)
    if config.is_dev:
        import asyncio
        from hypercorn.asyncio import serve
        from hypercorn.config import Config
        setup_logger()

        conf = Config()
        conf.bind ="0.0.0.0:8777"
        asyncio.run(serve(app, conf))
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8777)


