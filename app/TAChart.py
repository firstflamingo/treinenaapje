# coding=utf-8
#
#  Copyright (c) 2013-2015 First Flamingo Enterprise B.V.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  TAChart.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 11-Mar-13.
#

import webapp2
import logging, math

from datetime               import timedelta
from google.appengine.ext   import db
from google.appengine.api   import memcache

from TABasics               import TAModel, TAResourceHandler, JSONProperty
from TAScheduledPoint       import Direction

# ====== Chart Model ============================================================================

class TAChart(TAModel):
    
    # Stored attributes:
    _routePoints    = JSONProperty()
    _dataDictionary = JSONProperty()

    # Object lifecycle:
    @classmethod
    def new(cls, id):
        object = cls(key_name=id)
        object._dataDictionary = {}
        return object

    # Inserting data:
    
    def add_mission(self, mission):
        for stop in mission.stops:
            if stop.station_id:
                self.addPatternTime(stop.station_id, stop.up, mission.pattern_minutes_at_stop(stop))
                self.addDelay(stop.station_id, stop.up, stop.delay_dep)
                self.addPlatform(stop.station_id, stop.up, stop.platform)
    
    def addPatternTime(self, pointID, up, data):
        self.addOccurrence(patternTable(up), pointID, data)

    def addDelay(self, pointID, up, data):
        self.addOccurrence(delayTable(up), pointID, data)
    
    def addPlatform(self, pointID, up, data):
        if not data: return
        if '-' in data: return
        data = data.lower()
        self.addOccurrence(platformTable(up), pointID, data)

    def addOccurrence(self, tableName, pointID, data):
        histogram = self.histogramForPoint(tableName, pointID)
        data = str(data)
        histogram[data] = histogram.get(data, 0) + 1

    def histogramForPoint(self, tableName, pointID):
        table = self.tableWithName(tableName)
        histogram = table.get(pointID, None)
        if histogram == None:
            histogram = {}
            table[pointID] = histogram
        return histogram

    def tableWithName(self, tableName):
        table = self._dataDictionary.get(tableName, None)
        if table == None:
            table = {}
            self._dataDictionary[tableName] = table
        return table

    # Applying data to correct scheduledPoints
    
    def verifyPoint(self, point):
        self.processPatternStats(point, Direction.up)
        self.processPatternStats(point, Direction.down)
        self.processPlatformStats(point, Direction.up)
        self.processPlatformStats(point, Direction.down)

    def processPatternStats(self, point, direction):
        histogram = self.histogramForPoint(patternTable(direction), point.station_id)
        if histogram:
            arrival, departure = point.times_in_direction(direction)
            delta = int(check_value(departure, histogram)) - departure
            if delta == 0:
                pass
            elif -30 < delta < 30:
                arrival += delta
                departure += delta
                point.set_times_in_direction(direction, (arrival, departure))
                point.needs_datastore_put = True
                logging.warning('Point %s changed scheduled time in direction %d with %d' %
                                (point.id, direction, delta))
            else:
                logging.warning('Point %s found scheduled time delta of %d in direction %d. No changes were applied.' %
                                (point.id, delta, direction))
    
    def processPlatformStats(self, point, direction):
        histogram = self.histogramForPoint(platformTable(direction), point.station_id)
        count = 0.0
        for value in histogram.itervalues():
            count += value
        if count:
            platforms = []
            for key, value in histogram.iteritems():
                if value / count > 0.35:
                    platforms.append(key)
            platforms.sort()
            if platforms != point.platform_list[direction]:
                point.platform_list[direction] = platforms
                point.needs_datastore_put = True

    # Statistics

    def delayHist(self, pointID, direction):
        return self.histogramForPoint(delayTable(direction), pointID)

    def delayStats(self, pointID, direction):
        histogram = self.histogramForPoint(delayTable(direction), pointID)
        count = 0
        total = 0.0
        for key, value in histogram.iteritems():
            count += value
            total += float(key) * value
        if count == 0: return None, None
        avarage = total / count
        for key, value in histogram.iteritems():
            total += value * ((float(key) - avarage) ** 2)
        deviation = math.sqrt(total / count)
        return avarage, deviation

# ====== Chart Handler ===========================================================================

class TAChartHandler(TAResourceHandler):
    resourceClass = TAChart


# ====== Table names =============================================================================

def patternTable(up):
    if up:  return 'pattern_up'
    else:   return 'pattern_down'

def delayTable(up):
    if up:  return 'delay_up'
    else:   return 'delay_down'

def platformTable(up):
    if up:  return 'platform_up'
    else:   return 'platform_down'


# ====== Helper functions =========================================================================

def check_value(originalValue, histogram):
    foundValue, counter = most_common_item(histogram)
    if counter < 10:
        return originalValue
    else:
        return foundValue

def most_common_item(histogram):
    maxValue = 0
    foundKey = None
    for key, value in histogram.iteritems():
        if value > maxValue:
            foundKey = key
            maxValue = value
    return (foundKey, maxValue)


# ====== WSGI Application ========================================================================

URL_SCHEMA = [('/TAChart.*', TAChartHandler)]
app = webapp2.WSGIApplication(URL_SCHEMA, debug=True)
