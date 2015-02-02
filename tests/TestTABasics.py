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
#  TestTABasics.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 22-Feb-13.
#

"""TestTABasics.py contains a series of tests for TABasics"""

import logging, unittest

from datetime               import datetime
from google.appengine.api   import taskqueue, memcache
from google.appengine.ext   import db, testbed

from ffe.gae                import read_counter, increase_counter, counter_dict
from ffe.ffe_time           import UTC, CET, mark_utc, mark_cet, utc_from_cet, cet_from_utc

from TABasics               import TAModel

class TestFFEModules(unittest.TestCase):
    
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG

    def tearDown(self):
        self.testbed.deactivate()

    def test_march_conversion(self):
        winter_time = mark_cet(datetime(2013, 3, 30, 12))
        winter_utc = utc_from_cet(winter_time)
        self.assertEqual(winter_utc, mark_utc(datetime(2013, 3, 30, 11)))
        
        summer_time = datetime(2013, 3, 31, 10, tzinfo=UTC())
        summer_cet = cet_from_utc(summer_time)
        self.assertEqual(summer_cet, datetime(2013, 3, 31, 12, tzinfo=CET()))
    
    def test_october_conversion(self):
        summer_time = datetime(2013, 10, 26, 10, tzinfo=UTC())
        summer_cet = cet_from_utc(summer_time)
        self.assertEqual(summer_cet, datetime(2013, 10, 26, 12, tzinfo=CET()))
        
        winter_time = mark_cet(datetime(2013, 10, 27, 12))
        winter_utc = utc_from_cet(winter_time)
        self.assertEqual(winter_utc, mark_utc(datetime(2013, 10, 27, 11)))

    def test_counter(self):
        self.assertEqual(read_counter('test_id'), 0)
        increase_counter('test_id')
        increase_counter('test_id')
        self.assertEqual(read_counter('test_id'), 2)
        self.assertEqual(counter_dict(), {'mission_changes': 0, 'mission_no_changes': 0, 'mission_small_changes': 0,
                                          'req_api_success': 0, 'req_api_total': 0, 'req_avt_answered': 0,
                                          'req_avt_denied': 0, 'req_check_confirmed': 0, 'req_check_denied': 0,
                                          'req_check_refetched': 0, 'req_check_revoked': 0, 'req_departures': 0,
                                          'req_mission': 0, 'req_prio_answered': 0, 'req_prio_denied': 0,
                                          'req_trajectory': 0})


class TestTAModel(unittest.TestCase):

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
        """
        FRS 6.1 TAModel defines the identity of an object with code, country and id
        FRS 6.2 TAModel must create, store, fetch and delete objects
        """
        object = TAModel.new(code='test')

        # Assert derived properties are coming through:
        self.assertEqual(object.id, 'nl.test')
        self.assertEqual(object.code, 'test')
        self.assertEqual(object.country, 'nl')

        # Assert object can be memcached:
        object.cache_set()
        cached_object = memcache.get('nl.test', namespace='TAModel')
        self.assertEqual(cached_object.id, 'nl.test')

        # Assert 'get' works for memcached objects:
        requested_object = TAModel.get('nl.test')
        self.assertEqual(requested_object.id, 'nl.test')

        # Assert station can be stored:
        object.put()
        stored_object = db.get(db.Key.from_path('TAModel', 'nl.test'))
        self.assertEqual(stored_object.id, 'nl.test')

        # Assert object can be deleted
        object.delete()
        requested_object = TAModel.get('nl.test')
        cached_object = memcache.get('nl.test', namespace='TAModel')
        stored_object = db.get(db.Key.from_path('TAModel', 'nl.test'))
        self.assertEqual(requested_object, None)
        self.assertEqual(cached_object, None)
        self.assertEqual(stored_object, None)

    def test_objects_lists(self):
        """
        FRS 6.3 TAModel must provide lists to access its objects

        """
        objects = []
        for index in range(10):
            code = 'obj%d' % (9 - index)
            object = TAModel.new(code=code)
            objects.append(object)
        db.put(objects)

        ordered_ids = TAModel.all_ids()
        self.assertEqual(len(ordered_ids), 10)
        self.assertEqual(ordered_ids[9], 'nl.obj9')

        cached_ids = memcache.get('TAModel_ids')
        self.assertEqual(cached_ids, ordered_ids)

        objects_for_page = TAModel.paginatedObjects(page=2, length=3)
        self.assertEqual(len(objects_for_page), 3)
        first_object = objects_for_page[0]
        self.assertEqual(first_object.id, 'nl.obj3')

    def test_task_creation(self):

        object = TAModel.new('nl.obj')
        issue_time = datetime(2014, 2, 2, 14, 33)

        # standard instruction task
        task = object.instruction_task('/TABasics/url', 'test', issue_time)
        self.assertEqual(task.name, '02_1333_00_test_obj')
        self.assertEqual(task.url, '/TABasics/url')
        self.assertEqual(task.eta.replace(tzinfo=None), datetime(2014, 2, 2, 13, 33))
        self.assertEqual(task.payload, 'inst=test&sender=nl.obj')

        # instruction task with randomized seconds
        task = object.instruction_task('/TABasics/url', 'test', issue_time, random_s=True)
        self.assertEqual(task.name, '02_1333_xx_test_obj')
        self.assertEqual(task.eta.replace(second=0, tzinfo=None), datetime(2014, 2, 2, 13, 33))

        # instruction task with expected time
        expected = datetime(2014, 2, 2, 15, 03)
        task = object.instruction_task('/TABasics/url', 'test', issue_time, expected=expected)
        self.assertEqual(task.payload, 'expected=2014-02-02T15%3A03%3A00&inst=test&sender=nl.obj')

        # standard forward stop task
        task = object.stop_task(TAStopStub(), issue_time)
        self.assertEqual(task.name, '02_1333_00_fwd_obj')
        self.assertEqual(task.url, '/TAMission/nl.stub')
        self.assertEqual(task.eta.replace(tzinfo=None), datetime(2014, 2, 2, 13, 33))
        self.assertEqual(task.payload, '{"mi": "nl.stub"}')


class TAStopStub(object):

    @property
    def mission_id(self):
        return 'nl.stub'

    @property
    def repr(self):
        return {'mi': self.mission_id}

