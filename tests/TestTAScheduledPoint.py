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
#  TestTAScheduledPoint.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 12-Apr-14.
#

"""TestTAScheduledPoint.py contains a series of tests for TAScheduledPoint"""

import logging, unittest
from google.appengine.api   import memcache
from google.appengine.ext   import db, testbed
from TAScheduledPoint import TAScheduledPoint, Direction
from TASeries import TASeries


class TestTAScheduledPoint(unittest.TestCase):

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

    def test_stops_importer(self):
        series_007 = TASeries.new('nl.007')
        series_007.put()

        # Read the first data set
        xml_string = open('TestTAScheduledPoint.data/points1.xml', 'r').read()
        TAScheduledPoint.parse_schedule(xml_string, series_007)

        query = db.Query(TAScheduledPoint).filter('series_id =', 'nl.007').order('km')
        array = query.fetch(100)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0].station_id, 'nl.gvc')
        self.assertEqual(array[1].station_id, 'nl.shl')
        self.assertEqual(array[2].station_id, 'nl.asdz')

        point_shl = TAScheduledPoint.get('nl.007_shl')
        self.assertEqual(point_shl.series_id, 'nl.007')
        self.assertEqual(point_shl.station_id, 'nl.shl')
        self.assertEqual(point_shl.stationName, 'Schiphol')
        self.assertEqual(point_shl.km, 42.351)
        self.assertEqual(point_shl.upArrival, 63)
        self.assertEqual(point_shl.upDeparture, 63)
        self.assertEqual(point_shl.downArrival, 176)
        self.assertEqual(point_shl.downDeparture, 176)
        self.assertEqual(point_shl.platform_string(Direction.up), '2')
        self.assertEqual(point_shl.platform_string(Direction.down), '5')

        # Read the second data set
        xml_string = open('TestTAScheduledPoint.data/points2.xml', 'r').read()
        TAScheduledPoint.parse_schedule(xml_string, series_007)

        query = db.Query(TAScheduledPoint).filter('series_id =', 'nl.007').order('km')
        array = query.fetch(100)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0].station_id, 'nl.ledn')
        self.assertEqual(array[1].station_id, 'nl.shl')
        self.assertEqual(array[2].station_id, 'nl.asdz')

        point_shl = TAScheduledPoint.get('nl.007_shl')
        self.assertEqual(point_shl.series_id, 'nl.007')
        self.assertEqual(point_shl.station_id, 'nl.shl')
        self.assertEqual(point_shl.stationName, 'Schiphol')
        self.assertEqual(point_shl.km, 42.351)
        self.assertEqual(point_shl.upArrival, 65)
        self.assertEqual(point_shl.upDeparture, 65)
        self.assertEqual(point_shl.downArrival, 175)
        self.assertEqual(point_shl.downDeparture, 175)
        self.assertEqual(point_shl.platform_string(Direction.up), '1-2')
        self.assertEqual(point_shl.platform_string(Direction.down), '5-6')

    def test_ids_searching(self):
        point1 = TAScheduledPoint.new_with('nl.030', 'nl.ah')
        point1.put()
        point2 = TAScheduledPoint.new_with('nl.035', 'nl.ah')
        point2.put()
        point3 = TAScheduledPoint.new_with('nl.036', 'nl.ah')
        point3.put()
        point4 = TAScheduledPoint.new_with('nl.036', 'nl.zl')
        point4.put()

        expected = ['nl.030', 'nl.035', 'nl.036']
        ah_ids = TAScheduledPoint.series_ids_at_station('nl.ah')
        self.assertEqual(expected, ah_ids)
        cached_ids = memcache.get('series_ids@nl.ah')
        self.assertEqual(cached_ids, expected)