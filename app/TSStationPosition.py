# coding=utf-8
#
#  Copyright (c) 2014-2015 First Flamingo Enterprise B.V.
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
#  TSStationPosition.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 13-May-14.
#

"""TSStationPosition represents the position of a station on a route"""

import re
import logging
from google.appengine.ext import ndb
from ffe.rest_resources import Resource


class TSStationPosition(Resource):
    auto_creates_indexes = False
    publicly_visible = True
    station_key = ndb.KeyProperty(kind='TSStation')
    route_key = ndb.KeyProperty(kind='TSRoute')
    km = ndb.FloatProperty(default=0.0)
    geo_point = ndb.GeoPtProperty()
    platform_range = ndb.TextProperty()
    identifier_regex = re.compile('([a-z]{2})\.([a-z]{1,5})_([a-z]{2,4}[0-9]{1,2})$')

    # ------------ Object lifecycle ------------------------------------------------------------------------------------

    @classmethod
    def new(cls, identifier=None, country='nl', station_code=None, route_code='xx00'):
        if station_code and not identifier:
            identifier = '%s.%s_%s' % (country, station_code, route_code)
        self = super(TSStationPosition, cls).new(identifier)
        station_id = '%s.%s' % (self.country, self.station_code)
        self.station_key = ndb.Key('TSStation', station_id)
        route_id = '%s.%s' % (self.country, self.route_code)
        self.route_key = ndb.Key('TSRoute', route_id)
        return self

    # ------------ Object metadata -------------------------------------------------------------------------------------

    def __repr__(self):
        return "<%s %s = km %.3f on %s>" % (self.__class__.__name__, self.station_id, self.km, self.route_id)

    @property
    def country(self):
        return self.id_part(1)

    @property
    def station(self):
        return self.station_key.get()

    @property
    def station_code(self):
        return self.id_part(2)

    @property
    def station_id(self):
        return self.station_key.id()

    @property
    def route(self):
        return self.route_key.get()

    @property
    def route_code(self):
        return self.id_part(3)

    @property
    def route_id(self):
        return self.route_key.id()

    @property
    def coordinate(self):
        if self.geo_point is not None:
            return self.geo_point.lat, self.geo_point.lon
        else:
            return None, None

    @coordinate.setter
    def coordinate(self, coord):
        lat, lon = coord
        self.geo_point = ndb.GeoPt(lat, lon)

    def dictionary_from_object(self, perspective=None):
        dictionary = {'km': self.km, 'lat': self.coordinate[0], 'lon': self.coordinate[1]}

        if perspective != 'route':
            dictionary['route'] = self.route_id
        elif perspective != 'station':
            dictionary['station'] = self.station_id

        if self.platform_range is not None:
            dictionary['platforms'] = self.platform_range

        return dictionary
