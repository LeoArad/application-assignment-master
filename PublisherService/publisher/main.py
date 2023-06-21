import uvloop
from aiohttp import web
from publisher.app import init_app


def main():
    uvloop.install()
    app = init_app()
    web.run_app(app, port=80, access_log=None)  # access_log=None will turn off requests access logging


if __name__ == "__main__":
    main()
