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
#  TestTAChart.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 21-May-13.
#

"""TestTAChart.py contains a series of tests for TAChart"""

import logging, unittest, json

from google.appengine.api   import memcache
from google.appengine.ext   import db, testbed

from TAScheduledPoint   import TAScheduledPoint, Direction
from TAChart            import TAChart

class TestTASeries(unittest.TestCase):
    
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        
        logger = logging.getLogger()
        logger.level = logging.DEBUG
    
    def tearDown(self):
        self.testbed.deactivate()

    def test_chart(self):

        point = TAScheduledPoint.new_with('nl.055', 'nl.dld')
        point.scheduled_times = (17, 18, 24, 25)
        self.assertEqual(point.scheduled_times, (17, 18, 24, 25))
        self.assertEqual(point.platform_string(Direction.up), '-')

        chart = TAChart.new('nl.055_201301')
        for departure in [15, 15, 16, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17]:
            chart.addPatternTime('nl.dld', Direction.up, departure)
        for departure in [20, 22, 22, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]:
            chart.addPatternTime('nl.dld', Direction.down, departure)
        for delay in [10.0, 8.0, 7.0, 5.0, 5.0, 3.0, 3.0, 3.0, 2.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]:
            chart.addDelay('nl.dld', Direction.up, delay)
        for platform in ['1', '1', '3', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A', '5A']:
            chart.addPlatform('nl.dld', Direction.up, platform)
        for platform in ['7', '7', '7', '7', '7', '7', '7', '7', '7', '7', '7', '7', '', '', '', '', '', '',
                         '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8', '7-8',
                         '8', '8', '8', '8', '8', '8', '8', '8', '8', '8', '8', '12']:
            chart.addPlatform('nl.dld', Direction.down, platform)
        chart.put()

        chart = chart.get('nl.055_201301')
        expected = {
            'pattern_up': {'nl.dld': {'15': 2, '17': 24, '16': 1}},
            'platform_up': {'nl.dld': {'1': 2, '3': 1, '5a': 17}},
            'delay_up': {'nl.dld': {'5.0': 2, '10.0': 1, '1.0': 3, '8.0': 1, '0.0': 14, '7.0': 1, '3.0': 3, '2.0': 1}},
            'platform_down': {'nl.dld': {'8': 11, '12': 1, '7': 12}},
            'pattern_down': {'nl.dld': {'20': 1, '21': 24, '22': 2}}}
        result = chart._dataDictionary
        self.assertEqual(expected, result,
                         "FRS 11.3.1 TAChart must store operational data of the series.\nExpected: %s\nResult:   %s"
                         % (expected, result))

        chart.verifyPoint(point)
        self.assertEqual(point.scheduled_times, (16, 17, 20, 21),
                         "FRS 11.4.1 TAChart must verify a TAScheduledPoint")
        self.assertEqual(point.platform_string(Direction.up), '5a',
                         "FRS 11.4.1 TAChart must verify a TAScheduledPoint")
        self.assertEqual(point.platform_string(Direction.down), '7-8',
                         "FRS 11.4.1 TAChart must verify a TAScheduledPoint")
        self.assertTrue(point.needs_datastore_put)
        avarage, deviation = chart.delayStats('nl.dld', Direction.up)
        self.assertAlmostEqual(avarage, 1.8846, 4,
                               "FRS 11.5.1 TAChart must provide delay stats")
        self.assertAlmostEqual(deviation, 3.1234, 4,
                               "FRS 11.5.1 TAChart must provide delay stats")

