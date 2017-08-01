#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pika
import json
from bson.son import SON
from pymongo import MongoClient
import pymongo
from geopy.distance import great_circle
from geographiclib import geodesic
import redis
import multiprocessing as mp
from shared.analysis_functions import calculate_bearings_and_turns
import os
import sys
import logging
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler
import boto3

logging_config = dict(
    version=1,
    formatters={
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
    handlers={
        'h': {'class': 'logging.handlers.RotatingFileHandler',
              'formatter': 'f',
              'filename': 'log_queue-consumer.log',
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


coord_chg = 0.0001
queue_name = 'real_time'
min_surveillance_score = 100

with open("vrscreds.json", 'r') as credentials_file:
    vrs_credentials = json.loads(credentials_file.read())


def callback(ch, method, properties, body, r, dbmongo):
    """Add msg content to a flight path, or pull surveillance list if EOF."""
    # logger.debug(" [x] Received %r" % (body,))
    message_dict = json.loads(body.decode())
    if 'FileTime' in message_dict:  # if we get the special 5 minute marker message on the queue, time to mark stale flights
        pull_surveillance_flights(message_dict['FileTime'], r)
        clean_stale_flights(message_dict['FileTime'], r, dbmongo)
    else:
        flight_merger(message_dict, r, dbmongo)
    # logger.debug(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def coordinate_uniqueness_check(coordinate_list):
    """Checks if list of coordinates has any unique values."""
    for i, j in zip(coordinate_list, coordinate_list[1:]):
        if i[0:1] != j[0:1]:
            return 1
    return 0


def coordinate_lossy_compression(coordinate_list):
    """Selectively discards coordinates that do not convey much information."""
    coordinate_list.sort(key=lambda coord: coord[3])
    if len(coordinate_list) < 2:
        return coordinate_list
    else:
        compressed_coordinate_list = []
        compressed_coordinate_list.append(coordinate_list[0])
        for i in coordinate_list:
            # if float(i[3]) > (compressed_coordinate_list[-1][3] + 0.5): # drop coordinates within 0.5 seconds of each other
            if abs(i[0] - compressed_coordinate_list[-1][0]) + abs(i[1] - compressed_coordinate_list[-1][1]) > coord_chg:
                compressed_coordinate_list.append(i)
        # logger.debug("Dropped '{0}' of '{1}' coordinates!".format(len(coordinate_list)-len(compressed_coordinate_list), len(coordinate_list)))
        return compressed_coordinate_list


def clean_stale_flights(file_time, r, dbmongo):
    """Writes data for flights that have dropped from our data feed and removes them from redis."""
    stale_flight_list = r.zrangebyscore('flight_scan_times', '0', file_time - 3600)
    flights_to_mongo = []
    for i in stale_flight_list:
        flight_dict = json.loads(r.get(i))
        r.delete(i)
        r.zrem("flight_scan_times", i)
        r.zrem("surveillance_score", i)
        if len(flight_dict['geometry']['coordinates']) > 0 and len(flight_dict['geometry']['coordinates'][-1]) >= 2:  # not sure why this check is needed.
            airport_check_value = airport_proximity_check(flight_dict['geometry']['coordinates'][-1][0:2], r)
        else:
            airport_check_value = 0
        if airport_check_value == 0:
            flight_dict['FlightStatus'] = 'OutOfRange'
        else:
            flight_dict['LandedAirportID'] = airport_check_value
            flight_dict['FlightStatus'] = 'Landed'
        flight_dict['geometry']['coordinates'] = coordinate_lossy_compression(flight_dict['geometry']['coordinates'])
        if len(flight_dict['geometry']['coordinates']) > 1:
            flights_to_mongo.append(flight_dict)
    if len(flights_to_mongo) > 0:
        dbmongo.flighthistory.insert_many(flights_to_mongo, ordered=False)
    else:
        logger.debug("No flights inserted")
    return 0


def airport_proximity_check(coordinate, r):
    '''Query redis for nearby airports and return closest one, or 0 if none within range.'''
    if abs(coordinate[1]) >= 85.05112878:
        return 0
    else:
        airports_in_range = r.georadius('airports', *coordinate, radius=5000, unit='m', withdist=False, withcoord=False, withhash=False, count=1, sort='ASC')
    if len(airports_in_range) == 0:
        return 0
    else:
        return airports_in_range[0]


def landed_check(coordinate_list, r):
    '''Check if the plane has not moved fast enough recently to be airborne.'''
    # first do naive check - two points distance / time
    naive_distance = great_circle(coordinate_list[0][0:1], coordinate_list[-1][0:1]).meters
    time_delta = coordinate_list[-1][3] - coordinate_list[0][3]
    if time_delta < 200:
        return 0
    elif (naive_distance / time_delta) < 8.94:
        # logger.debug("Something is moving too slow to be flying!")
        return airport_proximity_check((coordinate_list[-1][0], coordinate_list[-1][1]), r)
    else:
        return 0


def extended_landing_check(coordinate_list, r):
    '''Run landed_check for multiple segments of the flight.'''
    check_index = [None] * 5
    check_index[0] = time_index_search(coordinate_list, 300)
    if check_index[0] == 0:  # start with a check over 5 minutes
        return 0  # if it's moving too fast, we can stop
    else:
        airport_result = landed_check(coordinate_list[check_index[0]:-1], r)
        if airport_result != 0:
            # additional scrutiny required to confirm landing - must be moving slowly over each minute
            for i in reversed(range(1, 4)):
                check_index[i] = time_index_search(coordinate_list, 60*i)
                if check_index[i] == check_index[i-1]:
                    return 0
                airport_result_loop = landed_check(coordinate_list[check_index[i-1]:check_index[i]], r)
                if airport_result_loop == 0:
                    return 0
    return airport_result


def time_index_search(coordinate_list, seconds):
    '''Look for coordinate matching a given time back, and return its index.'''
    if len(coordinate_list) < 5:
        return 0
    if coordinate_list[-1][3] - coordinate_list[0][3] < seconds:
        logger.debug("Coordinate list too short")
        return 0
    coordinate_count = 0
    for i in reversed(coordinate_list):
        coordinate_count += 1
        if coordinate_list[-1][3] - i[3] >= seconds:
            return len(coordinate_list) - coordinate_count


def pull_surveillance_flights(timestamp, r):
    """Pulls flight data for aircraft with a surveillance score above threshold, writes files, and uploads to S3."""
    try:
        logger.debug("Connecting to AWS S3 - secret key found")
        s3 = boto3.client('s3',
                          aws_access_key_id=vrs_credentials['aws_access_key_id'],
                          aws_secret_access_key=vrs_credentials['aws_secret_access_key'],
                          config=boto3.session.Config(signature_version='s3v4'))
    except:
        logger.debug("S3 connection failed")

    surveillance_aircraft_list = r.zrangebyscore('surveillance_score', min_surveillance_score, '+inf', withscores=True)
    logger.debug("surveillance aircraft list dump")
    logger.debug(surveillance_aircraft_list)

    # write local data files
    with open('latest.json', 'w+') as latest_file:
        json.dump({'TimeStamp': timestamp}, latest_file, sort_keys=True, indent=4)
    s3.upload_file(Bucket="nstarpost-flightmap-east2", Key="latest.json", Filename="latest.json")
    try:
        os.mkdir(str(timestamp))
    except:
        e = sys.exc_info()[0]
        logger.debug(e)
    aircraft_list_file_path = str(timestamp) + '/' + 'aircraft_list_' + str(timestamp) + '.json'
    with open('./' + aircraft_list_file_path, 'w') as aircraft_list_file:
        json.dump({'Aircraft': surveillance_aircraft_list}, aircraft_list_file, sort_keys=True, indent=4)
    try:
        upload_result = s3.upload_file(Bucket="nstarpost-flightmap-east2", Key=aircraft_list_file_path, Filename=('./' + aircraft_list_file_path))
    except:
        logger.debug(upload_result)
    for aircraft in surveillance_aircraft_list:
        flight_file_path = str(timestamp) + '/' + str(aircraft[0]) + '_' + str(timestamp) + '.json'
        with open('./' + flight_file_path, 'w') as flight_file:
            flight_file.write(str(r.get(aircraft[0])))  # take json sring and write out to file
        try:
            upload_result = s3.upload_file(Bucket="nstarpost-flightmap-east2", Key=flight_file_path, Filename=('./' + flight_file_path))
        except:
            logger.debug(upload_result)
            # logger.debug("aircraft info from redis")
            # logger.debug(r.get(aircraft[0]))


def flight_merger(flight_snippet_dict, r, dbmongo):
    """Takes flight snippets and appends them to flight paths or creates new flight records."""
    # Check if the dictionary contains useful info
    if len(flight_snippet_dict['geometry']['coordinates']) < 2:
        return 1
    elif coordinate_uniqueness_check(flight_snippet_dict['geometry']['coordinates']) == 0:
        return 1

    # If the aircraft is not tracked in redis, add it
    if r.exists(flight_snippet_dict['Icao']) == 0:
        # Set some initial information about the aircraft
        flight_snippet_dict['FlightStatus'] = 'InFlight'
        flight_snippet_dict['LandedScan'] = 0
        flight_snippet_dict['LandedAirportID'] = None

        # compress and calculate bearings / turns. should be ok with single coordinate
        flight_snippet_dict['geometry']['coordinates'] = coordinate_lossy_compression(flight_snippet_dict['geometry']['coordinates'])
        bearing_dict = calculate_bearings_and_turns(flight_snippet_dict['geometry']['coordinates'], flight_snippet_dict['geometry']['coordinates'][0])
        # replace existing coordinate list with compressed list that includes bearings
        flight_snippet_dict['geometry']['coordinates'] = bearing_dict['coordinates']
        # record last turn point, # of turns, and surveillance_score
        flight_snippet_dict['LastTurnPoint'] = bearing_dict['LastTurnPoint']
        flight_snippet_dict['LiveTurns'] = bearing_dict['new_turns']
        flight_snippet_dict['SurveillanceScore'] = bearing_dict['surveillance_score_incr']

        # put the last seen time into redis ranked list
        r.zadd("flight_scan_times", flight_snippet_dict['LastSeen'], flight_snippet_dict['Icao'])

        # assign a surveillance score in another ranked list
        r.zadd("surveillance_score", flight_snippet_dict['SurveillanceScore'], flight_snippet_dict['Icao'])
        # put flight_snippet_dict into redis, as a json string
        r.set(flight_snippet_dict['Icao'], json.dumps(flight_snippet_dict))
    else:
        # the aircraft is already tracked in redis. update flight info in redis
        existing_flight_dict = json.loads(r.get(flight_snippet_dict['Icao']))
        landed_scan_count = int(existing_flight_dict['LandedScan']) + 1

        update_flight_dict = dict(existing_flight_dict)
        new_coordinates = existing_flight_dict['geometry']['coordinates']

        # walk through new snippet coordinates and add the ones we don't have
        for x in flight_snippet_dict['geometry']['coordinates']:
            if x not in new_coordinates and x[2] != 0:  # disregard coordinate if altitude = 0
                new_coordinates.append(x)
        # check for bounds and then compress the coordinates we added.
        if len(new_coordinates) > 1:
            new_coordinates = coordinate_lossy_compression(new_coordinates)  # this could be optimized
        # now calculate bearings and put results in update_flight_dict
        bearing_dict = calculate_bearings_and_turns(new_coordinates, existing_flight_dict['LastTurnPoint'])
        new_coordinates = bearing_dict['coordinates']
        update_flight_dict['LastTurnPoint'] = bearing_dict['LastTurnPoint']
        update_flight_dict['LiveTurns'] += bearing_dict['new_turns']
        update_flight_dict['SurveillanceScore'] += bearing_dict['surveillance_score_incr']

        new_geometry = dict(existing_flight_dict['geometry'])
        new_geometry['coordinates'] = new_coordinates

        update_flight_dict['geometry'] = new_geometry
        update_flight_dict['LastSeen'] = flight_snippet_dict['LastSeen']
        update_flight_dict['LandedScan'] += 1

        # check if the aircraft landed
        if landed_scan_count >= 10:
            # landing_check gets the airport ID of the nearest airport in range, or 0 if none
            landing_check = extended_landing_check(new_coordinates, r)
            if landing_check != 0:
                logger.debug("The plane landed")
                update_flight_dict['FlightStatus'] = 'Landed'
                update_flight_dict['LandedAirportID'] = landing_check
                existing_flight_dict.update(update_flight_dict)

                # if this has some coordinates, write flight to mongodb
                if len(existing_flight_dict['geometry']['coordinates']) > 1:
                    dbmongo.flighthistory.insert_one(existing_flight_dict)

                # delete from redis and exit function
                r.delete(flight_snippet_dict['Icao'])
                r.zrem("flight_scan_times", flight_snippet_dict['Icao'])
                r.zrem("surveillance_score", flight_snippet_dict['Icao'])
                return 0
            else:
                # reset the counter
                update_flight_dict['LandedScan'] = 0

        # update redis
        r.zadd("flight_scan_times", update_flight_dict['LastSeen'], update_flight_dict['Icao'])
        r.zadd("surveillance_score", update_flight_dict['SurveillanceScore'], update_flight_dict['Icao'])
        r.set(update_flight_dict['Icao'], json.dumps(update_flight_dict))
    return 0


def consume():
    """Creates mongo, redis, and rabbitmq connections; consumes queue."""
    logger.debug("Consume started")
    redis_host = 'localhost'
    redis_port = 6379
    # connect to mongodb
    client = MongoClient()
    dbmongo = client.rt_flights_test
    # connect to redis
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
    # connect to rabbitmq and create queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    task_queue = channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    # start pulling data off the queue
    channel.basic_consume(lambda ch, method, properties, body: callback(ch, method, properties, body, r, dbmongo), queue=queue_name)
    channel.start_consuming()
    client.close()
    return 0


def main():
    """Launches consume() function in n multiple processes, where n = number of cores."""
    os.chdir("/opt/output-json")
    cores = mp.cpu_count()
    jobs = []
    for i in range(cores):
        p = mp.Process(target=consume)
        jobs.append(p)
        p.start()


main()
