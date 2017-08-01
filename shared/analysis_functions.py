#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 19 13:06:29 2017

@author: jason
"""

from geographiclib import geodesic
import logging
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler

logging_config = dict(
    version=1,
    formatters={
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
    handlers={
        'h': {'class': 'logging.handlers.RotatingFileHandler',
              'formatter': 'f',
              'filename': 'log_analysis-functions.log',
              'maxBytes': 4096,
              'backupCount': 10,
              'level': logging.DEBUG}
        },
    root={
        'handlers': ['h'],
        'level': logging.DEBUG,
        },
)

dictConfig(logging_config)

logger = logging.getLogger()


def score_altitude(alt):  # altitudes in feet
    """Returns a score for a given altitude of a turn."""
    if alt is None:
        return 0
    elif (alt <= 0.0 or alt > 55000.0):  # bad data and U-2s (out of scope)
        return 0
    elif (alt > 0.0 and alt <= 1000.0):  # taxxing
        return 1
    elif (alt > 1000.0 and alt <= 4000.0):  # coming in to land
        return 2
    elif (alt > 4000.0 and alt <= 6000.0):  # flight school, surveillance, tours
        return 3
    elif (alt > 6000.0 and alt <= 7000.0):  # more likely surveillance
        return 5
    elif (alt > 7000.0 and alt <= 12000.0):  # sweet spot for the Feds
        return 10
    elif (alt > 12000.0 and alt <= 17000.0):  # higher altitude national guard / military
        return 5
    elif (alt > 17000.0 and alt <= 23000.0):  # high altitude surveillance & commercial
        return 3
    elif (alt > 23000.0 and alt <= 25000.0):  # probably commercial traffic / holding patterns
        return 2
    elif (alt > 25000.0 and alt <= 55000.0):  # likely commercial jet traffic (holding patterns)
        return 1
    else:
        return 0


def calculate_bearings_and_turns(coordinates, last_turn_point):
    """Takes a list of coordinates and finds turns."""
    # if we don't have a bearing to start with, start with a bearing that will always be more than 90 degrees off
    # first point should count as the first turn
    surveillance_score_incr = 0
    if len(last_turn_point) == 4 or (len(last_turn_point) == 5 and last_turn_point[4] is None):
        last_turn_bearing = 999.0
        turn_count = -1
    # otherwise we start with the bearing from the last turn, and a count of 0
    else:
        turn_count = 0
        last_turn_bearing = last_turn_point[4]
    # now loop through all but the last coordinate
    for i in range(len(coordinates)-1):
        # if we already have a bearing, do nothing
        if len(coordinates[i]) == 5:
            pass
        # otherwise calculate the bearing and determine if a turn happened
        else:
            longitude1 = coordinates[i][0]
            latitude1 = coordinates[i][1]
            longitude2 = coordinates[i+1][0]
            latitude2 = coordinates[i+1][1]
            current_bearing = geodesic.Geodesic.WGS84.Inverse(lon1=longitude1, lat1=latitude1, lon2=longitude2, lat2=latitude2)['azi1']
            # if the bearing calc is negative, add 360 to make positive
            if current_bearing < 0.0:
                current_bearing += 360.0
            # now add the bearing to the point
            coordinates[i].append(round(current_bearing, 4))
            # compare the difference between the current bearing and last turn. If > 90, count as a turn
            if abs(max(last_turn_bearing, current_bearing) - min(last_turn_bearing, current_bearing)) > 90.0:
                altitude = coordinates[i][2]
                surveillance_score_incr += score_altitude(altitude)
                print("Turn ", turn_count, " identified. Point ", last_turn_point, " to ", coordinates[i+1])
                turn_count += 1
                last_turn_bearing = current_bearing
                last_turn_point = coordinates[i+1]
    if turn_count < 0:
        turn_count = 0
    # logger.debug("Turn count: {0}".format(turn_count))
    return {"coordinates": coordinates, "LastTurnPoint": last_turn_point, "new_turns": turn_count, "surveillance_score_incr": surveillance_score_incr}
