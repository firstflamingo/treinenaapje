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
#  TestTAStop.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 25-Feb-13.
#

"""TestTAStop.py contains a series of tests for TAStop"""

import logging, unittest, json

from datetime import datetime
from google.appengine.api import memcache
from google.appengine.ext import ndb, testbed

from TAStop import TAStop, StopStatuses, NSRespondsWithError
from TSStation import TSStation


class TestTAStop(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG

    def tearDown(self):
        self.testbed.deactivate()

    def test_object_creation(self):
        stop = TAStop()
        stop.status = StopStatuses.announced
        stop.departure = datetime(2013, 2, 25, 16, 14)
        stop.platform = '7a'
        self.assertEqual(json.dumps(stop.repr), '{"p": "7a", "v": "2013-02-25T16:14:00"}')
        stop.status = StopStatuses.extra
        stop.delay_dep = 1.0
        stop.platformChange = True
        self.assertEqual(json.dumps(stop.repr), '{"dv": 1.0, "pc": "7a", "s": 1, "v": "2013-02-25T16:14:00"}')

    def test_repr(self):
        """
        FRS 13.3 TAStop must support serialization

        """
        repr = {}
        repr['si'] = "nl.asd"
        repr['mi'] = "nl.2641"
        repr['s'] = StopStatuses.extra
        repr['a'] = "2013-02-25T16:12:00"
        repr['v'] = "2013-02-25T16:14:00"
        repr['dv'] = 1.0
        repr['de'] = "Vlissingen"
        repr['ad'] = "Rotterdam"
        repr['p'] = "7a"
        stop = TAStop.fromRepr(repr)
        reprCopy = stop.repr
        self.assertEqual(repr, reprCopy)

    def test_derived_properties(self):
        stop = TAStop()
        stop.station_id = 'nl.asd'
        station = TSStation.new('nl.asd')
        station.put()
        self.assertEqual(stop.station.id_, station.id_)

        stop.mission_id = 'nl.2641'
        self.assertEqual(stop.number, 2641)
        self.assertEqual(stop.up, 1)

        stop.arrival = datetime(2013, 2, 25, 16, 12)
        stop.departure = datetime(2013, 2, 25, 16, 14)
        stop.delay_dep = 2.0
        self.assertEqual(stop.arrival_string, '16:12')
        self.assertEqual(stop.departure_string, '16:14')
        self.assertEqual(stop.est_departure.strftime('%H:%M'), '16:16')

        stop.destination = 'Vlissingen'
        stop.alteredDestination = 'Roosendaal'
        self.assertEqual(stop.real_destination, 'Roosendaal')

    def test_stops_importer(self):
        station_stub = StationStub()

        # Read the first data set, 4 stops must be imported
        xml_string = open('TestTAStop.data/stops1.xml', 'r').read()
        changed_stops = TAStop.parse_avt(xml_string, delegate=station_stub)
        self.assertEqual(len(changed_stops), 4)
        self.assertEqual(len(station_stub.stops_dictionary), 4)
        self.assertEqual(station_stub.nr_of_fetches, 1)
        self.assertEqual(station_stub.last_departure.replace(tzinfo=None), datetime(2013, 2, 23, 15, 29))

        stop1 = station_stub.get_stop('501_test')
        self.assertEqual(stop1.station_id, 'nl.test')
        self.assertEqual(stop1.mission_id, 'nl.501')
        self.assertEqual(stop1.status, StopStatuses.announced)
        self.assertEqual(stop1.now, None)
        self.assertEqual(stop1.arrival, None)
        self.assertEqual(stop1.departure.replace(tzinfo=None), datetime(2013, 2, 23, 14, 44))
        self.assertEqual(stop1.delay_arr, 0.0)
        self.assertEqual(stop1.delay_dep, 0.0)
        self.assertEqual(stop1.destination, 'Ede-Wageningen')
        self.assertEqual(stop1.alteredDestination, None)
        self.assertEqual(stop1.platform, '1')
        self.assertEqual(stop1.platformChange, False)

        stop2 = station_stub.get_stop('502_test')

        # Read the second data set and check the changes
        xml_string = open('TestTAStop.data/stops2.xml', 'r').read()
        changed_stops = TAStop.parse_avt(xml_string, delegate=station_stub)
        self.assertEqual(len(station_stub.stops_dictionary), 4)
        self.assertEqual(len(changed_stops), 3)
        self.assertEqual(station_stub.nr_of_fetches, 2)
        self.assertEqual(station_stub.last_departure.replace(tzinfo=None), datetime(2013, 2, 23, 15, 29))

        self.assertTrue(station_stub.get_stop('501_test') is None)
        self.assertTrue(station_stub.get_stop('502_test') is stop2)
        self.assertFalse(stop2 in changed_stops)

        stop3 = station_stub.get_stop('503_test')
        self.assertTrue(stop3 in changed_stops)
        self.assertEqual(stop3.status, StopStatuses.extra)
        self.assertEqual(stop3.delay_arr, 0.0)
        self.assertEqual(stop3.delay_dep, 2.0)
        self.assertEqual(stop3.destination, 'Ede-Wageningen')
        self.assertEqual(stop3.alteredDestination, 'het opstelspoor')
        self.assertEqual(stop3.platform, '2')
        self.assertEqual(stop3.platformChange, True)

        stop4 = station_stub.get_stop('504_test')
        self.assertTrue(stop4 in changed_stops)
        self.assertEqual(stop4.status, StopStatuses.canceled)

        stop5 = station_stub.get_stop('700504_test')
        self.assertEqual(stop5.status, StopStatuses.announced)

        xml_string = open('TestTAStop.data/error.xml', 'r').read()
        with self.assertRaises(NSRespondsWithError):
            TAStop.parse_avt(xml_string, delegate=station_stub)


class StationStub(object):
    _stops_dictionary = None
    nr_of_fetches = 0
    last_departure = None

    @property
    def station_id(self):
        return 'nl.test'

    @property
    def code(self):
        return 'test'

    @property
    def name(self):
        return 'Station Stub'

    @property
    def stops_dictionary(self):
        if self._stops_dictionary is None:
            self._stops_dictionary = {}
        return self._stops_dictionary

    @stops_dictionary.setter
    def stops_dictionary(self, value):
        self._stops_dictionary = value

    def cache_set(self):
        pass

    def get_stop(self, key):
        return self.stops_dictionary.get(key)
