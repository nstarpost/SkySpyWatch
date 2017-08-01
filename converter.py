# -*- coding: utf-8 -*-
from datetime import datetime
import json
from pymongo import MongoClient
import time
from geopy.distance import great_circle
import pika

from os import listdir, chdir
from os.path import isfile, join

import requests

import logging
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler

write_debug_json_files = 0


logging_config = dict(
    version=1,
    formatters={
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
    handlers={
        'h': {'class': 'logging.handlers.RotatingFileHandler',
              'formatter': 'f',
              'filename': 'log_converter-rt.log',
              'maxBytes': 4096,
              'backupCount': 3,
              'level': logging.DEBUG}
        },
    root={
        'handlers': ['h'],
        'level': logging.DEBUG,
        },
)

dictConfig(logging_config)
logger = logging.getLogger()

queue_name = 'real_time'  # use a different queue for the real-time stuff

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
task_queue = channel.queue_declare(queue=queue_name, durable=True)
channel.basic_qos(prefetch_count=1)

lastDv = None
with open("vrscreds.json", 'r') as credentials_file:
    vrs_credentials = json.loads(credentials_file.read())


def enqueue_flight_snippet(flight_snippet):
    """Add items from the flight_dictionary to rabbitmq queue as json strings"""
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=flight_snippet,
                          properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
                          )


# this function reads a dictionary of a flight snapshot and returns a different and easier to work with dictionary
def flight_snapshot_scanner(flight_dictionary):
    """Translates a dictionary of a flight snapshot to be easier to use and places on queue."""
    flightdict_0 = {}
    flightdict_0['meta'] = {}
    flightdict_0['meta']['IcaoDict'] = {}
    for item in flight_dictionary['acList']:
        if 'Cos' in item:
            if item['TT'] == 'a' and len(item['Cos']) > 0:
                i = 0
                position_list = []
                while i < len(item['Cos']):
                    # the incoming list of coordinates is a repeating sequence of
                    # latitude, longitude, time (unix * 1000), altitude
                    # we need to calculate bearings and order this for geojson
                    longitude = item['Cos'][i+1]
                    latitude = item['Cos'][i]
                    altitude = item['Cos'][i+3]
                    unix_time = item['Cos'][i+2]/1000
                    if i+4 < len(item['Cos']):
                        longitude_next = item['Cos'][i+5]
                        latitude_next = item['Cos'][i+4]
                        # bearing = geodesic.Geodesic.WGS84.Inverse(lon1=longitude, lat1=latitude, lon2=longitude_next, lat2=latitude_next)['azi1']
                    else:
                        pass
                        # bearing = None
                        # bearing = position_list[-1][3]  # use the previous bearing if this is the last position in the flight - perhaps make null instead
                    coordinate_list = [longitude,
                                       latitude,
                                       altitude,
                                       unix_time,
                                       ]
                    # divide by 1000 above to get unix time
                    #if coordinate_list[2] is None:
                    #    coordinate_list[2] = 0  # need to figure out why geojson isn't letting me give null altitudes
                    # filter out invalid coordinates
                    if abs(coordinate_list[0]) <= 180 and abs(coordinate_list[1]) <= 90:
                        position_list.append(coordinate_list)
                    i += 4
                # check if there are still coordinates left after filtering
                if len(position_list) > 0:
                    flight_id = item['Icao'] + '_' + str(int(round(item['Cos'][2] / 1000, 0)))
                    item_copy = item.copy()
                    del item_copy['Cos']
                    # item_copy['positions'] = position_list
                    # add the positions as geojson instead
                    item_copy['geometry'] = {"type": "LineString", "coordinates": position_list}
                    item_copy['LastSeen'] = position_list[len(position_list)-1][3]
                    flightdict_0[flight_id] = item_copy
                    # send the flight snippet to rabbitmq
                    #logger.debug(item_copy)
                    enqueue_flight_snippet(json.dumps(item_copy))
                    flightdict_0['meta']['IcaoDict'][item['Icao']] = flight_id
            else:
                logger.debug("No altitude!")
    return flightdict_0


def req_aircraft_inflight():
    """Makes a request to VirtualRadarServer's HTTP JSON api and enqueues flight snippets followed by EOF."""
    client = MongoClient()
    dbmongo = client.rt_flights_test
    logger.debug(dbmongo.flighthistory.create_index([("geometry", "2dsphere")]))
    request_url = "http://localhost:8080/VirtualRadar/AircraftList.json"
    request_params = {
                        'trFmt': 'sa',
                        'refreshTrails': 1
                    }

    snapshot_req = requests.put(request_url, params=request_params, auth=(vrs_credentials['username'], vrs_credentials['password']))
    if snapshot_req.ok:
        snapshot_dict = json.loads(snapshot_req.content.decode())
        # logger.debug(phx_snapshot_dict)
        flight_snapshot_scanner(snapshot_dict)
        lastDv = snapshot_dict['lastDv'] # this is a timestamp used by the server to identify updates
        time_marker = {'FileTime': round(time.time())}
        enqueue_flight_snippet(json.dumps(time_marker))
        if write_debug_json_files == 1:
            with open('/opt/converter-debug/vrssnapshot_' + str(round(time.time())) + '.json', 'w') as request_json:
                request_json.write(snapshot_req.content.decode()) # take json sring and write out to file

    else:
        logger.error("Request to {0} failed with status code {1}".format(request_url, snapshot_req.status_code))


def main():
    """Requests flight data from VirtualRadarServer every 60 seconds."""
    logger.debug("Process started!")
    count = 0
    while True:
        req_aircraft_inflight()
        logger.debug("Request {0} complete!".format(count))
        count += 1
        connection.sleep(60)
    return 0


main()
