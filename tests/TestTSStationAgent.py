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
#  TestTSStationAgent.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 30-Apr-14.
#

import logging, unittest
import webapp2, webtest
from datetime               import timedelta, datetime
from ffe                import config
from ffe.ffe_time       import CET, mark_cet, string_from_cet, now_cet
from ffe.gae            import read_counter
from google.appengine.api import memcache
from google.appengine.ext import testbed
from agent_api import AGENT_URL_SCHEMA
from TSStationAgent import TSStationAgent
from TAStop import TAStop, StopStatuses


class TestTSStation(unittest.TestCase):

    def setUp(self):
        app = webapp2.WSGIApplication(AGENT_URL_SCHEMA)
        self.testapp = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG

    def tearDown(self):
        self.testbed.deactivate()

    def test_object_creation(self):
        now = now_cet()
        agent = TSStationAgent.get('nl.test')
        logging.info('New agent: %s' % agent)
        agent.updated = now
        agent.cache_set()

        agent = TSStationAgent.get('nl.test')
        self.assertEqual(agent.id_, 'nl.test')
        self.assertEqual(agent.code, 'test')
        self.assertEqual(agent.country, 'nl')
        self.assertEqual(agent.updated, now)
        logging.info('Updated agent: %s' % agent)

    def test_update_instructions(self):
        """
        FRS 8.3 TSStationAgent must process update requests
        FRS 8.8 Removing a station
        """
        now = mark_cet(datetime(2013, 2, 23, 14, 30))
        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        # Post instruction to test avt file, with 10 new stops:
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'avt',
                           'file': 'TestTSStationAgent.data/avt-edc.xml',
                           'now': string_from_cet(now)})
        self.assertEqual(read_counter('req_avt_answered'), 1)
        self.assertEqual(read_counter('req_api_success'), 1)

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 10)
        numbers = []
        for task in tasks:
            url = task.get('url')
            numbers.append(int(url[-3:]))
        numbers.sort()
        self.assertEqual(numbers, range(337, 347))

        ede_centrum = TSStationAgent.get('nl.edc')
        self.assertEqual(ede_centrum.last_departure.strftime('%H:%M'), '16:59')
        self.assertTrue(isinstance(ede_centrum.last_departure.tzinfo, CET))
        self.assertEqual(ede_centrum.updated.strftime('%H:%M'), '14:30')
        self.assertTrue(isinstance(ede_centrum.updated.tzinfo, CET))
        self.assertEqual(len(ede_centrum.sorted_stops), 10)

        # Given the state of the station, any instructions to fetch avt again must be ignored:
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'avt',
                           'now': string_from_cet(now)})
        self.assertEqual(read_counter('req_avt_denied'), 1)

        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'prio',
                           'now': string_from_cet(now)})
        self.assertEqual(read_counter('req_prio_denied'), 1)

        # When time proceeds, instructions to fetch avt must be responded to:
        period = config.MIN_INTERVAL_BEFORE_PRIO_REQ + 1
        poll_time = now + timedelta(minutes=period)
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'prio',
                           'now': string_from_cet(poll_time)})
        self.assertEqual(read_counter('req_prio_answered'), 1)

        period = config.MIN_PERIOD_STORED_DEPARTURES - 1
        poll_time = ede_centrum.last_departure - timedelta(minutes=period)
        response = self.testapp.post('/agent/station/nl.edc',
                                     {'inst': 'avt',
                                      'now': string_from_cet(poll_time)})
        self.assertEqual(read_counter('req_avt_answered'), 2)

        ede_centrum.updated = poll_time
        ede_centrum.cache_set()
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'avt',
                           'now': string_from_cet(poll_time)})
        self.assertEqual(read_counter('req_avt_denied'), 2)

        period = config.MIN_INTERVAL_BEFORE_AVT_REQ + 1
        poll_time += timedelta(minutes=period)
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'avt',
                           'now': string_from_cet(poll_time)})
        self.assertEqual(read_counter('req_avt_answered'), 3)

        # An avt request sent via the console must be answered to anyway and result in a redirect
        # When the fetched avt data didn't change, no update instructions should be forwarded:
        taskq.FlushQueue('default')
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'console',
                           'file': 'TestTSStationAgent.data/avt-edc.xml',
                           'now': string_from_cet(now)})
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 0)

        # When the fetched data has changes, the changes must be detected and forwarded:
        self.testapp.post('/agent/station/nl.edc',
                          {'inst': 'console',
                           'file': 'TestTSStationAgent.data/avt-edc2.xml',
                           'now': string_from_cet(now)})
        tasks = taskq.GetTasks('default')
        for task in tasks:
            logging.debug('%s ==> %s' % (task.get('name'), task.get('url')))

        self.assertEqual(len(tasks), 10)

        ede_centrum = TSStationAgent.get('nl.edc')
        stops = ede_centrum.stops_dictionary
        self.assertEqual(stops['31338_edc'].delay_dep, 2.0)
        self.assertEqual(stops['31339_edc'].platform, '5')
        self.assertEqual(stops['31339_edc'].status, StopStatuses.announced)
        self.assertEqual(stops['31339_edc'].platformChange, True)
        self.assertEqual(stops['41340_edc'].status, StopStatuses.extra)
        self.assertEqual(stops['31341_edc'].status, StopStatuses.canceled)
        self.assertEqual(stops['31342_edc'].status, StopStatuses.canceled)
        self.assertEqual(stops['31343_edc'].status, StopStatuses.canceled)
        self.assertEqual(stops['31345_edc'].alteredDestination, 'Arnhem')
        self.assertEqual(stops['31346_edc'].alteredDestination, 'Barneveld Noord')

        # Check the counters respond correctly
        self.assertEqual(read_counter('req_api_total'), 0)
        self.assertEqual(read_counter('req_api_success'), 3)
        self.assertEqual(read_counter('req_avt_answered'), 3)
        self.assertEqual(read_counter('req_avt_denied'), 2)
        self.assertEqual(read_counter('req_prio_answered'), 1)
        self.assertEqual(read_counter('req_prio_denied'), 1)

    def test_announcement_check(self):
        """
        FRS 8.3.3 Checking and revoking stops

        """
        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        # Check the first train, while no stops have been fetched yet
        response = self.testapp.post('/agent/station/nl.edc',
                                     {'inst': 'check',
                                      'sender': 'nl.31337',
                                      'expected': '2013-02-23T14:44:00',
                                      'file': 'TestTSStationAgent.data/avt-edc3.xml',
                                      'now': '2013-02-23T13:30:00'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(read_counter('req_api_success'), 1)
        self.assertEqual(read_counter('req_check_refetched'), 1)

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].get('name'), '23_1230_03_fwd_edc')
        self.assertEqual(tasks[1].get('name'), '23_1230_07_fwd_edc')
        mission_urls = ['/TAMission/nl.31337', '/TAMission/nl.31338']
        self.assertTrue(tasks[0].get('url') in mission_urls)
        self.assertTrue(tasks[1].get('url') in mission_urls)
        taskq.FlushQueue('default')

        # Check the second train, which should be available in memory:
        response = self.testapp.post('/agent/station/nl.edc',
                                     {'inst': 'check',
                                      'sender': 'nl.31338',
                                      'expected': '2013-02-23T14:59:00',
                                      'file': 'TestTSStationAgent.data/avt-edc3.xml',
                                      'now': '2013-02-23T13:31:00'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(read_counter('req_api_success'), 1)
        self.assertEqual(read_counter('req_check_confirmed'), 1)

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].get('name'), '23_1231_03_fwd_edc')
        self.assertEqual(tasks[0].get('url'), '/TAMission/nl.31338')
        taskq.FlushQueue('default')

        # Check the creation of a revoked stop
        revoked_stop = TAStop.revoked_stop('nl.31339', 'nl.edc')
        self.assertEqual(revoked_stop.station_id, 'nl.edc')
        self.assertEqual(revoked_stop.mission_id, 'nl.31339')
        self.assertEqual(revoked_stop.status, StopStatuses.revoked)

        # Check the third train, which not available at all, but within reach of the current download:

        response = self.testapp.post('/agent/station/nl.edc',
                                     {'inst': 'check',
                                      'sender': 'nl.31335',
                                      'expected': '2013-02-23T14:14:00',
                                      'file': 'TestTSStationAgent.data/avt-edc3.xml',
                                      'now': '2013-02-23T13:32:00'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(read_counter('req_api_success'), 1)
        self.assertEqual(read_counter('req_check_denied'), 1)

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].get('name'), '23_1232_03_fwd_edc')
        self.assertEqual(tasks[0].get('url'), '/TAMission/nl.31335')
        taskq.FlushQueue('default')

        # Check the fourth train, which is not available at all and out of reach of the current download:

        response = self.testapp.post('/agent/station/nl.edc',
                                     {'inst': 'check',
                                      'sender': 'nl.31339',
                                      'expected': '2013-02-23T15:14:00',
                                      'file': 'TestTSStationAgent.data/avt-edc3.xml',
                                      'now': '2013-02-23T13:32:00'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(read_counter('req_api_success'), 2)
        self.assertEqual(read_counter('req_check_revoked'), 1)

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].get('name'), '23_1232_03_fwd_edc')
        self.assertEqual(tasks[0].get('url'), '/TAMission/nl.31339')
