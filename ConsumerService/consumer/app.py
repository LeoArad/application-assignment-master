import asyncio
import json
import os

import aio_pika
import psycopg2
import pytz

from aio_pika import connect, ExchangeType
from aiohttp import web
from aiojobs.aiohttp import setup
from collections import defaultdict
from datetime import datetime, timezone
from typing import Union


async def get_db_conn() -> psycopg2.connect:
    return psycopg2.connect(
        host="0.0.0.0",
        port="5432",
        database=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


async def setup_rabbitmq() -> tuple[aio_pika.connect, aio_pika.queue]:
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
    return connection, queue


async def consume_events(queue) -> None:
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                body = json.loads(message.body.decode())
                if body:
                    print(f"Received message: {body}")
                    try:
                        event_time_obj = datetime.strptime(body['event_time'], "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        print(f"The is a problem with the row {body} event_time format")
                    await insert_row_to_db(body['p_id'], body['medication_name'], body['action'],
                                           event_time_obj.timestamp())


async def execute_command(query, params, select=True) -> None:
    db_conn = await get_db_conn()
    cursor = db_conn.cursor()
    try:
        cursor.execute(query, params) if params else cursor.execute(query)
        if select:
            return cursor.fetchall()
        db_conn.commit()
    except psycopg2.Error as pgerror:
        print(f"Error in insertion operation error: {pgerror}")
    finally:
        db_conn.close()


async def insert_row_to_db(p_id, medication_name, action, event_time) -> None:
    params = (p_id, medication_name, action, event_time)
    insert_query = """INSERT INTO public.meds(
                    p_id, medication_name, action, event_time)
                    VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"""

    select_query = """SELECT p_id, medication_name, action, event_time
                    FROM public.meds
                    WHERE p_id= %s AND medication_name=%s AND action=%s AND event_time=%s;"""
    if not await execute_command(select_query, params):
        await execute_command(insert_query, params, False)
        print(f"Insert row to db p_id: {p_id}, medication_name: {medication_name}, action: {action}, "
              f"event_time: {event_time}")
    else:
        print("Row already exists")


async def parse_inner_logic(type: str, row: tuple, meds_object: defaultdict) -> None:
    row_datetime_obj = datetime.fromtimestamp(row[3])
    utc=pytz.UTC
    if type in row[2]:
        if row[2] == type:
            if not meds_object[row[1]].get(type) or datetime.strptime(meds_object[row[1]].get(type), "%Y-%m-%d %H:%M:%S") < row_datetime_obj.astimezone(timezone.utc).replace(tzinfo=None):
                meds_object[row[1]][type] = row_datetime_obj.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        else:
            if type in meds_object[row[1]]:
                del meds_object[row[1]][type]


async def parse_start_and_end(row: tuple, meds_object: defaultdict) -> None:
    await parse_inner_logic('start', row, meds_object) if 'start' in row[2] else await parse_inner_logic('stop', row,
                                                                                                   meds_object)


async def parse_db_results_to_periods(results) -> defaultdict:
    meds_object = defaultdict(dict)
    for row in results:
        await parse_start_and_end(row, meds_object)
    return meds_object


async def get_report_from_meds_object(meds_object: defaultdict, p_id: int) -> str:
    report_result = [f"Report for p_id {p_id}"]
    for k, v in meds_object.items():
        if 'start' in v and 'stop' in v and v['start'] < v['stop']:
            report_result.append(f"Medicine {k} start time {v['start']} and ends {v['stop']}")
        elif 'start' not in v and 'stop' in v:
            report_result.append(f"The Medicine {k}  have only end time")
        elif 'start' in v and 'stop' not in v:
            report_result.append(f"The Medicine {k} have only start time")
        elif 'start' in v and 'stop' in v and v['start'] > v['stop']:
            report_result.append(f"The start time and stop time of Medicine {k} is wrong")
    return '\n'.join(report_result) if len(report_result) > 1 else f"Didn't found any Medicines for p_id {p_id}"


async def get_periods(request) -> Union[web.HTTPBadRequest, web.Response]:
    p_id = request.match_info.get('p_id')
    select_query = """SELECT p_id, medication_name, action, event_time
                        FROM public.meds
                        WHERE p_id= %s;"""
    p_results = await execute_command(select_query, (p_id, ))
    if not p_results:
        return web.HTTPBadRequest(text=f"There is no p_id {p_id}")
    results_dict = await parse_db_results_to_periods(p_results)
    report = await get_report_from_meds_object(results_dict, p_id)
    return web.Response(text=report)


async def get_default_page(request) -> web.Response:
    return web.Response(text="Must add p_id")


def init_app():
    app = web.Application()
    app.router.add_route("GET", "/", get_default_page)
    app.router.add_route("GET", "/{p_id}", get_periods)
    setup(app)
    return app


async def on_startup():
    connection, queue = await setup_rabbitmq()
    await consume_events(queue)
    await connection.close()

