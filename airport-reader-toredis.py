#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from csv import reader
import redis

# connect to redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)


def check_coordinate_redis(coordinate):
    if abs(coordinate[0]) >= 180.0 or abs(coordinate[1]) >= 85.05112878:
        return 1
    else:
        return 0


with open('./data/ourairports-2017_02_19.csv', newline='', encoding='utf-8') as csvfile:
    filereader = reader(csvfile, delimiter=',')
    airport_list = []
    for row in filereader:
        if is_number(row[4]):
            if abs(float(row[5])) <= 180.0 and abs(float(row[4])) <= 85.05112878:
                airport_list.append(row[5])  # long
                airport_list.append(row[4])  # lat
                airport_list.append(row[0])  # airportID
            else:
                print("Skipped ", row[0], " due to coordinates out of range")
    redis_status = r.geoadd('airports', *airport_list)
    print(redis_status)
