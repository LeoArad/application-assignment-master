import asyncio
import json
import os

from aio_pika import connect, ExchangeType, Message, DeliveryMode
from aiohttp import web
from aiojobs.aiohttp import setup


async def setup_rabbitmq():
    connection = await connect(host=os.environ.get('RABBIT_HOST'),
                               login=os.environ.get('RABBIT_USER'),
                               password=os.environ.get('RABBIT_PASS'),
                               loop=asyncio.get_running_loop()
                               )

    # Creating a channel
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "meds", ExchangeType.FANOUT
    )
    queue = await channel.declare_queue(name='events', auto_delete=True)

    await queue.bind(exchange, "new.events")
    return connection, exchange


async def publish_events(exchange):
    with open("events.json") as f:
        events = json.load(f)
        for event in events:
            message = Message(
                json.dumps(event).encode(),
                delivery_mode=DeliveryMode.PERSISTENT
            )
            await exchange.publish(message, routing_key="new.events")
        print("pushed all events to RabbitMQ")


def init_app():
    app = web.Application()
    setup(app)
    app.on_startup.append(on_startup)
    return app


async def on_startup(app: web.Application):
    connection, exchange = await setup_rabbitmq()
    await publish_events(exchange)
    await connection.close()
