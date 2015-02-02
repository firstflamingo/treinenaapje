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
#  TestTAPublic.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 08-May-13.
#
# For documentation of webtest module see: http://webtest.pythonpaste.org/en/latest/webtest.html

"""TestTAPublic.py contains a series of tests for TAPublic"""

import logging, unittest
import webapp2, webtest

from google.appengine.api   import memcache
from google.appengine.ext   import db, testbed

from ffe.gae                import read_counter
import TAPublic
from TASeries import TASeries

class TestTAPublic(unittest.TestCase):
    
    def setUp(self):
        self.app = TAPublic.app
        self.testApp = webtest.TestApp(self.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        
        logger = logging.getLogger()
        logger.level = logging.DEBUG
    
    def tearDown(self):
        self.testbed.deactivate()
    
    def test_handler(self):
        self.assertFalse(self.app.debug,
                         "FRS 4.1.1 Debug-mode in TAPublic must be switched off")
        response = self.testApp.get('/trajectory', status=404)
        self.assertEqual(response.status, '404 Not Found',
                         "FRS 4.1.2 Requests with incomplete parameters must be answered with 404")
        response = self.testApp.get('/mission', status=404)
        self.assertEqual(response.status, '404 Not Found',
                         "FRS 4.1.2 Requests with incomplete parameters must be answered with 404")

    def test_trajectory(self):
        TASeries.import_xml('TestTAPublic.data/series_trajectory.xml')

        expected = '{"origin": "nl.ut", "destination": "nl.ehv", "options": [{"id": "nl.829", "v": "2013-05-16T09:08:00"}, {"id": "nl.3529", "v": "2013-05-16T09:23:00"}, {"id": "nl.831", "v": "2013-05-16T09:38:00"}, {"id": "nl.3531", "v": "2013-05-16T09:53:00"}]}'
        response = self.testApp.get('/trajectory?from=nl.ut&to=nl.ehv&start=2013-05-16T09:00:00&span=1')
        self.assertEqual(expected, response.body)  # "FRS 4.2.1 TAPublic must publish a list with travel options")

        expected = '{"origin": "nl.ut", "options": [{"id": "nl.829", "v": "2013-05-16T09:08:00"}, {"id": "nl.831", "v": "2013-05-16T09:38:00"}]}'
        response = self.testApp.get('/departures?series=nl.008&dir=up&from=nl.ut&start=2013-05-16T09:00:00&span=1')
        self.assertEqual(expected, response.body, "FRS 4.2.4 TAPublic must publish a list with departure times: %s" % response.body)

        expected = '{"origin": "nl.ut", "options": [{"id": "nl.828", "v": "2013-05-16T09:55:00"}]}'
        response = self.testApp.get('/departures?series=nl.008&dir=down&from=nl.ut&start=2013-05-16T09:00:00&span=1')
        self.assertEqual(expected, response.body, "FRS 4.2.4 TAPublic must publish a list with departure times: %s" % response.body)

        expected = '{"id": "nl.828", "stops": []}'
        response = self.testApp.get('/mission?id=nl.828')
        self.assertEqual(expected, response.body)

        self.assertEqual(read_counter('req_trajectory'), 1)
        self.assertEqual(read_counter('req_mission'), 1)
        self.assertEqual(read_counter('req_departures'), 2)
