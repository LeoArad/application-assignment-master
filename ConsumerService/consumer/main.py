import uvloop
import asyncio
from aiohttp import web
from consumer.app import init_app, on_startup


def main():
    uvloop.install
    loop = asyncio.get_event_loop()
    loop.create_task(on_startup())

    app = init_app()
    web_runner = web.AppRunner(app)
    loop.run_until_complete(web_runner.setup())
    site = web.TCPSite(web_runner, '0.0.0.0', 8112)
    loop.run_until_complete(site.start())
    loop.run_forever()


if __name__ == "__main__":
    main()
