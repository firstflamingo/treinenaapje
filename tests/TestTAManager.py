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
#  TestTAManager.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 21-Feb-13.
#

"""TestTAManager.py contains a series of tests for TAManager"""

import logging, unittest
import webapp2, webtest
from datetime import datetime, timedelta

from google.appengine.api   import taskqueue, memcache
from google.appengine.ext   import db, testbed

import TAManager

from ffe            import config
from ffe.ffe_time   import now_utc, mark_utc
from TSStation      import TSStation
from TASeries       import TASeries
from TAMission      import TAMission

class TestTAManager(unittest.TestCase):
    
    def setUp(self):
        app = webapp2.WSGIApplication(TAManager.URL_SCHEMA)
        self.testapp = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG
    
    def tearDown(self):
        self.testbed.deactivate()
    
    def test_station(self):
        "Manager must create TAStation catalog and issue tasks"
        
        test_station_list = ['nl.zd', 'nl.amr', 'nl.ut']
        for id in test_station_list:
            station = TSStation.new(id)
            station.put()
        station_list = TSStation.all_ids()
        test_station_list.sort()
        self.assertEqual(station_list, test_station_list)
        station_list = memcache.get('TSStation_ids')
        self.assertEqual(station_list, test_station_list)

        self.testapp.get('/TAManager/create_avt_tasks')

        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 3)

        old_time = now_utc()
        reference = config.WAIT_BEFORE_FIRST_TASK
        for task in tasks:
            new_time = mark_utc(datetime.strptime(task.get('eta'), '%Y/%m/%d %H:%M:%S'))
            interval = new_time - old_time
            self.assertAlmostEqual(interval.seconds, reference, -1)
            reference = 60.0 * config.STATION_AVT_DURATION / 3
            old_time = new_time

    def test_series(self):
        "Manager must create TASeries catalog and issue tasks"
        
        test_series_list = ['nl.022', 'nl.026']
        for identifier in test_series_list:
            series = TASeries.new(identifier)
            series.put()
        series_list = TASeries.all_ids()
        self.assertEqual(series_list, test_series_list)
        series_list = memcache.get('TASeries_ids')
        self.assertEqual(series_list, test_series_list)
    
        response = self.testapp.get('/TAManager/new_day')
        self.assertEqual(response.status, '200 OK')

        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 2)

        old_time = now_utc()
        reference = config.WAIT_BEFORE_FIRST_TASK
        for task in tasks:
            new_time = mark_utc(datetime.strptime(task.get('eta'), '%Y/%m/%d %H:%M:%S'))
            interval = new_time - old_time
            self.assertAlmostEqual(interval.seconds, reference, -1)
            reference = 60.0 * config.SERIES_CONSOLIDATION_DURATION / 2
            old_time = new_time

    def test_orphan_removal(self):

        missions = []
        nr_of_missions = 2
        for i in range(12001, 12001 + nr_of_missions):
            mission = TAMission.get(code=str(i), create=True)
            missions.append(mission)

        db.put(missions)
        output = db.Query(TAMission).filter('series_id =', 'orphan').fetch(1000)
        self.assertEqual(len(output), nr_of_missions)

        response = self.testapp.get('/TAManager/remove_orphans')
        self.assertEqual(response.status, '200 OK')

        mission = TAMission.get(code='12001')
        self.assertEqual(mission, None)

        output = db.Query(TAMission).filter('series_id =', 'orphan').fetch(1000)
        self.assertEqual(len(output), 0)
