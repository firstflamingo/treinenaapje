# coding=utf-8
#
#  Copyright (c) 2012-2015 First Flamingo Enterprise B.V.
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
#  TAScheduledPoint.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 14-Nov-12.
#

import logging, json, re
import xml.sax
from google.appengine.ext import db
from google.appengine.api import memcache

from ffe.markup     import XMLElement, XMLImporter
from ffe.ffe_time   import string_from_minutes, minutes_from_string
from TABasics       import TAModel

class Direction:
    down, up = range(2)


class TAScheduledPoint(TAModel):

    identifier_regex = re.compile('([a-z]{2})\.([0-9]{3})_([a-z]{1,5})$')

    # Stored attributes:
    series_id           = db.StringProperty()
    station_id          = db.StringProperty()
    km                  = db.FloatProperty()
    stationName         = db.StringProperty(indexed=False)
    upArrival           = db.IntegerProperty(indexed=False)
    upDeparture         = db.IntegerProperty(indexed=False)
    downArrival         = db.IntegerProperty(indexed=False)
    downDeparture       = db.IntegerProperty(indexed=False)
    platformData        = db.TextProperty()

    # Transient attributes
    needs_datastore_put = False
    _platformList = None
    
    # Object lifecycle:
    @classmethod
    def new_with(cls, seriesID, stationID):
        series_code = seriesID.split('.')[1]
        country, station_code = stationID.split('.')
        identifier = '%s.%s_%s' % (country, series_code, station_code)
        self = cls(key_name=identifier)
        self.series_id = seriesID
        self.station_id = stationID
        return self

    # Relations

    @classmethod
    def series_ids_at_station(cls, station_id):
        memcache_key = 'series_ids@%s' % station_id
        result = memcache.get(memcache_key)
        if result is None:
            result = []
            keys = db.Query(cls, keys_only=True).filter('station_id =', station_id).fetch(100)
            for key in keys:
                mo = cls.identifier_regex.match(key.name())
                series_id = '%s.%s' % (mo.group(1), mo.group(2))
                result.append(series_id)
            memcache.set(memcache_key, result)
        return result

    # Scheduled times:
    @property
    def scheduled_times(self):
        return (self.upArrival, self.upDeparture, self.downArrival, self.downDeparture)

    @scheduled_times.setter
    def scheduled_times(self, four_times):
        self.upArrival, self.upDeparture, self.downArrival, self.downDeparture = four_times

    @property
    def times_strings(self):
        up_string = '%d - %d' % (self.upArrival, self.upDeparture)
        down_string = '%d - %d' % (self.downArrival, self.downDeparture)
        return (up_string, down_string)
    
    def arrival_in_direction(self, direction):
        if direction:
            return self.upArrival
        else:
            return self.downArrival
    
    def departure_in_direction(self, direction):
        if direction:
            return self.upDeparture
        else:
            return self.downDeparture
    
    def times_in_direction(self, direction):
        if direction:
            return (self.upArrival, self.upDeparture)
        else:
            return (self.downArrival, self.downDeparture)

    def set_times_in_direction(self, direction, twoTimes):
        if direction:
            self.upArrival, self.upDeparture = twoTimes
        else:
            self.downArrival, self.downDeparture = twoTimes

    @property
    def station_code(self):
        comps = self.station_id.split('.')
        return comps[1]

    # Import and export
    @classmethod
    def parse_schedule(cls, xml_string, series):
        handler = ScheduleImporter()
        handler.series = series
        xml.sax.parseString(xml_string, handler)

    @property
    def station_xml(self):
        attributes = {'km': '%.3f' % self.km, 'id': self.station_id}
        if self.stationName:
            attributes['name'] = self.stationName
        return XMLElement('station', attributes)

    @property
    def up_xml(self):
        return XMLElement('up', {'station': self.station_id,
                                     'arr': string_from_minutes(self.upArrival),
                                     'dep': string_from_minutes(self.upDeparture),
                                'platform': self.platform_string(Direction.up)})

    @property
    def down_xml(self):
        return XMLElement('down', {'station': self.station_id,
                                       'arr': string_from_minutes(self.downArrival),
                                       'dep': string_from_minutes(self.downDeparture),
                                  'platform': self.platform_string(Direction.down)})

    # Packing and unpacking data

    @property
    def platform_list(self):
        if self._platformList != None: return self._platformList
        if self.platformData != None:
            self._platformList = json.loads(self.platformData)
        else:
            self._platformList = [[], []]
        return self._platformList

    def platform_string(self, direction):
        array = self.platform_list[direction]
        if array:
            return '-'.join(array)
        else:
            return '-'

    def set_platform_string(self, direction, value):
        if value == '-':
            array = []
        else:
            array = value.split('-')
        self.platform_list[direction] = array
        self.platformData = json.dumps(self.platform_list)


class ScheduleImporter(XMLImporter):

    series = None
    station_id = None
    station_name = None
    km = None
    arrival = None
    departure = None
    platform = None

    def active_xml_tags(self):
        return ['station', 'up', 'down']

    def existing_objects_dictionary(self):
        dictionary = {}
        for point in self.series.points:
            dictionary[point.station_id] = point
        return dictionary

    def key_for_current_object(self):
        return self.station_id

    def create_new_object(self, key):
        return TAScheduledPoint.new_with(self.series.id, self.station_id)

    def start_xml_element(self, name, attrs):
        if name == 'station':
            self.station_id = attrs.get('id')
            self.station_name = attrs.get('name')
            self.km = float(attrs.get('km'))

        elif name == 'up' or name == 'down':
            self.station_id = attrs.get('station')
            self.arrival = minutes_from_string(attrs.get('arr'))
            self.departure = minutes_from_string(attrs.get('dep'))
            self.platform = attrs.get('platform')

    def update_object(self, existing_object, name):
        if name == 'station':
            if existing_object.stationName != self.station_name:
                existing_object.stationName = self.station_name
                self.changes = True

            if existing_object.km != self.km:
                existing_object.km = self.km
                self.changes = True

        elif name == 'up':
            self.update_object_in_direction(existing_object, Direction.up)

        elif name == 'down':
            self.update_object_in_direction(existing_object, Direction.down)

    def update_object_in_direction(self, existing_object, direction):
        if existing_object.times_in_direction(direction) != (self.arrival, self.departure):
            existing_object.set_times_in_direction(direction, (self.arrival, self.departure))
            self.changes = True

        if existing_object.platform_string(direction) != self.platform:
            existing_object.set_platform_string(direction, self.platform)
            self.changes = True

    def save_objects(self):
        if self.old_objects:
            objects = self.old_objects.values()
            logging.info('Delete %d scheduledPoints.' % len(objects))
            memcache.delete_multi(TAScheduledPoint.dictionary_from_list(objects), namespace='TAScheduledPoint')
            db.delete(objects)
        if self.updated_objects:
            objects = self.updated_objects.values()
            logging.info('Update %d scheduledPoints.' % len(objects))
            memcache.set_multi(TAScheduledPoint.dictionary_from_list(objects), namespace='TAScheduledPoint')
            db.put(objects)
        if self.old_objects or self.updated_objects:
            self.series.reset_points()
