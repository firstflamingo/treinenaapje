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
#  TestTAMission.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 28-Jan-13.
#
#
# For documentation of webtest module see: http://webtest.pythonpaste.org/en/latest/webtest.html
#
# For testing taskqueue see: http://stackoverflow.com/questions/6632809/gae-unit-testing-taskqueue-with-testbed

"""TestTAMission.py contains a series of tests for TAMission"""

import sys, logging, unittest, json
import webapp2, webtest

from datetime               import time, date, datetime, timedelta
from google.appengine.api   import memcache, taskqueue
from google.appengine.ext   import db
from google.appengine.ext   import testbed

from ffe.gae            import read_counter
from ffe.ffe_time       import mark_cet, now_cet
from TASeries           import TASeries, SERIES_URL_SCHEMA
from TAMission          import TAMission, MissionStatuses
from TSStation          import TSStation
from TAStop             import StopStatuses
from TAScheduledPoint   import Direction

class TestTAMission(unittest.TestCase):

    def setUp(self):
        app = webapp2.WSGIApplication(SERIES_URL_SCHEMA)
        self.missionApp = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG

    def tearDown(self):
        self.testbed.deactivate()

    def test_object_fetch(self):

        # Create sample data:
        missionData = ['nl.2641', 'nl.2642', 'nl.2643']
        for id in missionData:
            mission = TAMission.new(id)
            mission.put()

            # Fetch object url and parse as JSON:
            response = self.missionApp.get('/TAMission/%s' % id)
            dictionary = json.loads(response.body)
            self.assertEqual(dictionary['id'], id)

        # Fetch the catalog:
        response = self.missionApp.get('/TAMission')
        self.assertEqual(response.body, '["nl.2641", "nl.2642", "nl.2643"]')

    def test_mission_basics(self):

        # Create series
        series_d14 = TASeries.new('eu.d14')
        series_d14.put()
        series123 = TASeries.new('nl.123')
        series123.put()
        memcache.delete('nl.123', namespace='TASeries')

        # Create international train
        mission241 = TAMission.get('eu.241', create=True)
        mission241.nominalDate = date(2010, 1, 31)
        mission241.offset_time = time(12, 0)
        self.assertTrue(mission241.needs_datastore_put,
                        "FRS 10.3.7 After changing offset_time mission must be marked as needs_datastore_put")
        mission241.put()

        # Create standard up train
        mission12301 = TAMission.get(code='12301', create=True)
        mission12301.offset_cet = mark_cet(datetime(2010, 1, 31, 14))
        mission12301.delay = 5.0
        self.assertTrue(mission12301.needs_datastore_put,
                        "FRS 10.3.7 After changing offset_time mission must be marked as needs_datastore_put")
        mission12301.put()

        series123 = mission12301.series
        self.assertEqual(series123.id, 'nl.123')

        # Create orphaned down train
        mission12402 = TAMission.get(code='12402', create=True)
        mission12402.put()

        # Create supplementary mission
        mission312301 = TAMission.get(code='312301', create=True)
        self.assertFalse(mission312301.needs_datastore_put, "Supplementary mission need no datastore put")

        # FRS 10.1 Mission creation
        self.assertNotEqual(mission12301, None,
                            "FRS 10.1.1 A mission must be created with the get command")
        self.assertEqual(mission241.series_id, 'eu.d14',
                         "FRS 10.1.2 Trainnumbers lower than 500 must be international trains")
        self.assertEqual(mission12301.series_id, 'nl.123',
                         "FRS 10.1.3 At creation, the correct series must be assigned")
        self.assertEqual(mission312301.series_id, 'nl.123',
                         "FRS 10.1.3 At creation, the correct series must be assigned")
        self.assertEqual(mission12402.series_id, 'orphan',
                         "FRS 10.1.3 When the corresponding series does not exist, the mission must be marked as orphan")

        # FRS 10.2 Properties deduced from train number
        self.assertTrue(mission12301.up,
                        "FRS 10.2.1 Missions with odd numbers are running up")
        self.assertFalse(mission12402.up,
                         "FRS 10.2.1 Missions with even numbers are running down")
        self.assertEqual(mission241.ordinal, 1,
                         "FRS 10.2.2 In international trains the last digit is the ordinal")
        self.assertEqual(mission12402.ordinal, 2,
                         "FRS 10.2.2 In normal trains the last two digits are the ordinal")
        self.assertEqual(mission241.series_number, 24,
                         "FRS 10.2.3 TAMission must deduce the series number")
        self.assertEqual(mission12301.series_number, 123,
                         "FRS 10.2.3 TAMission must deduce the series number")
        self.assertEqual(mission312301.series_number, 123,
                         "FRS 10.2.3 TAMission must deduce the series number")
        self.assertEqual(mission12301.base_number, 12301,
                         "FRS 10.2.4 The last five digits form the base number")
        self.assertEqual(mission312301.base_number, 12301,
                         "FRS 10.2.4 The last five digits form the base number")
        self.assertFalse(mission12301.supplementary,
                         "FRS 10.2.5 Missions with five digit numbers are not supplementary")
        self.assertEqual(mission312301.supplementary, 3,
                         "FRS 10.2.5 Missions with six digit numbers are supplementary")

        # FRS 10.3 Setting nominalDate and offset_time
        self.assertEqual(mission12301.nominalDate, date(2010, 1, 31),
                         "FRS 10.3.1 TAMission must provide nominalDate")
        self.assertEqual(mission12301.date_string, '31-01-2010',
                         "FRS 10.3.2 TAMission must provide dateString")
        self.assertEqual(mission12301.offset_time, time(14),
                         "FRS 10.3.3 TAMission must provide offset_time")
        self.assertEqual(mission12301.offset_string, '14:00',
                         "FRS 10.3.4 TAMission must provide offsetString")
        self.assertEqual(mission241.offset_cet, mark_cet(datetime(2010, 1, 31, 12)),
                         "FRS 10.3.5 TAMission must provide offset_cet")

        self.assertEqual(mission312301.offset_time, time(14),
                         "Supplementary mission must copy original mission offset")
        self.assertEqual(mission312301.delay, 0.0)

        testSet = (('09:57', '09:57'), ('09:58', '10:00'), ('10:02', '10:00'), ('10:03', '10:03'),
                   ('10:27', '10:27'), ('10:28', '10:30'), ('10:32', '10:30'), ('10:33', '10:33'))
        for (timeIn, timeOut) in testSet:
            mission12301.offset_cet = mark_cet(datetime.strptime('2010-01-31T' + timeIn, '%Y-%m-%dT%H:%M'))
            self.assertEqual(mission12301.offset_string, timeOut,
                             "FRS 10.3.6 offset_time must be rounded within two minutes %s -> %s" % (timeIn, timeOut))

        # Missions must be registered correctly
        series_d14 = TASeries.get('eu.d14')
        self.assertEqual(series_d14.all_mission_ids(Direction.up), ['eu.241'])
        series123 = TASeries.get('nl.123')
        self.assertEqual(series123.all_mission_ids(Direction.up), ['nl.12301', 'nl.312301'])

    def test_odids(self):

        # FRS 10.4 setting originID and destinationID

        # 1) Creating the dictionary
        mission = TAMission.new('nl.1234')
        self.assertEqual(mission.odIDs_dictionary, {'d': [None, None]})

        # 2) Reading and writing
        mission.nominalDate = now_cet().date()
        mission.odIDs_dictionary['d'] = ['nl.asd', 'nl.ehv']
        mission.origin_id = 'nl.asd'
        mission.destination_id = 'nl.ehv'
        self.assertFalse(mission.needs_datastore_put)
        mission.put()
        mission = TAMission.get('nl.1234')
        self.assertEqual(mission.origin_id, 'nl.asd')
        self.assertEqual(mission.destination_id, 'nl.ehv')
        self.assertEqual(mission.get_odIDs_string(0), 'nl.asd-nl.ehv')
        self.assertEqual(len(mission.odIDs_dictionary), 1)

        mission.set_odIDs_string(5, 'nl.asd-nl.ht')
        mission.set_odIDs_string(6, '-')
        self.assertTrue(mission.needs_datastore_put)
        self.assertEqual(mission.get_odIDs_for_weekday(5), ['nl.asd', 'nl.ht'])
        self.assertEqual(mission.get_odIDs_for_weekday(6), [None, None])
        self.assertEqual(len(mission.odIDs_dictionary), 3)

        # 3) Optimizing the dictionary
        for weekday in range(6):
            mission.set_odIDs_string(weekday, 'nl.asd-nl.mt')
        self.assertEqual(len(mission.odIDs_dictionary), 8)
        mission.optimize_odIDs_dictionary()
        self.assertEqual(len(mission.odIDs_dictionary), 2)
        self.assertTrue(mission.needs_datastore_put)

    def test_activate_mission(self):
        """
        FRS 10.5 Activating a mission and maintaining its status

        """
        # Load sample data:
        TSStation.update_stations('TestTAMission.data/stations.xml')
        TASeries.import_xml('TestTAMission.data/series.xml')

        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        # FRS 10.5.1 If not specified, activate_mission must set nominalDate to the current date
        now = now_cet().replace(hour=2)
        mission = TAMission.get('nl.3046')
        mission.activate_mission()
        self.assertEqual(mission.nominalDate, now.date())
        taskq.FlushQueue('default')

        # FRS 10.5.2/3 activate_mission must generate origin_id, destination_id and stops
        test_set = ((mark_cet(datetime(2013, 2, 24, 2)), None, 0),
                    (mark_cet(datetime(2013, 2, 18, 2)), 'nl.asd', 5),
                    (mark_cet(datetime(2013, 2, 19, 2)), 'nl.amr', 8))
        for (testDate, destination, nr_of_stops) in test_set:
            mission.activate_mission(testDate)
            self.assertEqual(mission.destination_id, destination)
            self.assertEqual(len(mission.stops), nr_of_stops)
        mission.put()
        self.assertEqual(mission.origin_id, 'nl.ah')
        self.assertEqual(mission.last_stop.arrival_string, '15:48')

        # FRS 10.5.4 activated stops must get 'planned' status, last stop 'finalDestination'
        for index in range(0, 6):
            self.assertEqual(mission.stops[index].status, StopStatuses.planned)
        self.assertEqual(mission.stops[7].status, StopStatuses.finalDestination)

        # FRS 10.5.5 TAMission must queue a check-task while awaking a mission.
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[1]['url'], '/TAMission/nl.3046')
        self.assertEqual(tasks[1]['name'], '19_1231_xx_check_3046')
        taskq.FlushQueue('default')

        # FRS 10.5.6 Mission must check announcement of stops
        check_time = mark_cet(datetime(2013, 2, 19, 13, 41, 22))
        mission.stops[0].status = StopStatuses.announced
        mission.check_mission_announcements(check_time)
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]['url'], '/agent/station/nl.ed')
        self.assertEqual(tasks[0]['name'], '19_1241_25_check_3046')
        self.assertEqual(tasks[1]['url'], '/TAMission/nl.3046')
        self.assertEqual(tasks[1]['name'], '19_1246_xx_check_3046')
        taskq.FlushQueue('default')

        check_time = mark_cet(datetime(2013, 2, 19, 14, 02, 22))
        mission.stops[0].status = StopStatuses.planned
        mission.stops[1].status = StopStatuses.announced
        mission.stops[2].status = StopStatuses.announced
        mission.stops[3].status = StopStatuses.announced
        mission.stops[4].status = StopStatuses.announced
        mission.check_mission_announcements(check_time)
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]['url'], '/TAMission/nl.3046')
        self.assertEqual(tasks[0]['name'], '19_1348_xx_check_3046')

        # FRS 10.5.7 Mission must provide status and delay
        (status, delay) = mission.status_at_time(mark_cet(datetime(2013,2,19,14,0)))
        self.assertEqual(status, MissionStatuses.inactive)
        self.assertEqual(delay, 0)
        mission.first_stop.status = StopStatuses.announced
        (status, delay) = mission.status_at_time(mark_cet(datetime(2013,2,19,14,0)))
        self.assertEqual(delay, 0)
        self.assertEqual(status, MissionStatuses.announced)
        (status, delay) = mission.status_at_time(mark_cet(datetime(2013,2,19,14,30)))
        self.assertEqual(status, MissionStatuses.running)
        self.assertEqual(delay, 0)
        (status, delay) = mission.status_at_time(mark_cet(datetime(2013,2,19,15,49)))
        self.assertEqual(mission.est_arrival_cet, mark_cet(datetime(2013,2,19,15,48)))
        self.assertEqual(status, MissionStatuses.arrived)
        self.assertEqual(MissionStatuses.s[status], 'arrived')
        self.assertEqual(delay, 0)

    def test_discover_mission(self):
        # Load sample data:
        TSStation.update_stations('TestTAMission.data/stations.xml')
        TASeries.import_xml('TestTAMission.data/series.xml')

        # FRS 10.6 Discover new mission, from scratch
        self.post_stops_from_file('TestTAMission.data/step_10_6a.json')
        mission_orphan = TAMission.get('nl.9046')
        self.assertNotEqual(mission_orphan, None,           "FRS 10.6.1 TAMission must receive updates")
        self.assertEqual(len(mission_orphan.stops), 1,      "FRS 10.6.2 TAMission must create new mission")

        self.post_stops_from_file('TestTAMission.data/step_10_6b.json')
        self.post_stops_from_file('TestTAMission.data/step_10_6c.json')
        mission_orphan = TAMission.get('nl.9046')
        self.assertEqual(len(mission_orphan.stops), 3,                  "FRS 10.6.3 TAMission must insert new stops")
        self.assertEqual(mission_orphan.stops[0].station_id, 'nl.ah',    "FRS 10.6.3 TAMission must place stops in the right order")
        self.assertEqual(mission_orphan.stops[1].station_id, 'nl.klp',   "FRS 10.6.3 TAMission must place stops in the right order")
        self.assertEqual(mission_orphan.stops[2].station_id, 'nl.ut',    "FRS 10.6.3 TAMission must place stops in the right order")

        for stop in mission_orphan.stops:
            self.assertEqual(stop.status, StopStatuses.announced,       "FRS 10.6.4 Updated stops must get 'announced' as status")

        memcache.delete('nl.9046', namespace='TAMission')
        mission_orphan = TAMission.get('nl.9046')
        self.assertEqual(len(mission_orphan.stops), 3,      "FRS 10.6.5 After significant changes, missions must be stored in the datastore")

        # FRS 10.7 Discover new mission, from series
        series = TASeries.get('nl.030')
        series.activate_new_day(mark_cet(datetime(2013, 2, 19, 2, 0)))

        # Check sample data
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44, None)

        # STEP 7a: insert Veenendaal De Klomp
        self.post_stops_from_file('TestTAMission.data/step_10_7a.json')
        mission_44 = TAMission.get('nl.3044')
        self.log_mission(mission_44)

        self.assertEqual(mission_44.series.id, 'nl.030')
        self.assertEqual(mission_44.offset_time.strftime('%H:%M'), '13:00')
        self.assertEqual(mission_44.origin_id, 'nl.klp')
        self.assertEqual(mission_44.destination_id, 'nl.asd')

        # FRS 10.7.3 Mission must create stops
        self.assertEqual(len(mission_44.stops), 3)
        stop_klp = mission_44.stops[0]
        self.assertEqual(stop_klp.station_id, 'nl.klp')
        self.assertEqual(stop_klp.status, StopStatuses.announced)
        self.assertEqual(stop_klp.departure.strftime('%H:%M'), '13:46')
        self.assertEqual(mission_44.stops[1].station_id, 'nl.ut')
        self.assertEqual(mission_44.stops[1].status, StopStatuses.planned)
        self.assertEqual(mission_44.stops[2].station_id, 'nl.asd')
        self.assertEqual(mission_44.stops[2].status, StopStatuses.finalDestination)
        self.assertEqual(mission_44.odIDs, ['nl.klp', 'nl.asd'])

        # STEP 7b: insert Arnhem
        self.post_stops_from_file('TestTAMission.data/step_10_7b.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44.origin_id, 'nl.ah')
        self.assertEqual(mission_44.destination_id, 'nl.asd')

        # FRS 10.7.4 Mission must insert new stops prior to current stops
        self.assertEqual(len(mission_44.stops), 5)
        self.assertEqual(mission_44.stops[0].station_id, 'nl.ah')
        self.assertEqual(mission_44.stops[0].status, StopStatuses.announced)
        self.assertEqual(mission_44.stops[1].station_id, 'nl.ed')
        self.assertEqual(mission_44.stops[1].status, StopStatuses.planned)
        self.assertEqual(mission_44.stops[4].station_id, 'nl.asd')
        self.assertEqual(mission_44.stops[4].status, StopStatuses.finalDestination)
        self.assertEqual(mission_44.odIDs, ['nl.ah', 'nl.asd'])

        # STEP 7c: update Ede Wageningen (should already be there)
        self.post_stops_from_file('TestTAMission.data/step_10_7c.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44.origin_id, 'nl.ah')
        self.assertEqual(mission_44.destination_id, 'nl.asd')

        # FRS 10.7.5 Mission must update stops
        self.assertEqual(len(mission_44.stops), 5)
        self.assertEqual(mission_44.stops[1].station_id, 'nl.ed')
        self.assertEqual(mission_44.stops[1].status, StopStatuses.announced)

        # FRS 10.7.6 Update from planned to announced may not cause datastore put
        memcache.delete('nl.3044', namespace='TAMission')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(len(mission_44.stops), 5)
        self.assertEqual(mission_44.stops[1].station_id, 'nl.ed')
        self.assertEqual(mission_44.stops[1].status, StopStatuses.planned)

        # STEP 7Rc: revoke Ede Wageningen
        self.post_stops_from_file('TestTAMission.data/step_10_7Rc.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(len(mission_44.stops), 4)

        # STEP 7c again: update Ede Wageningen, inserting a scheduled stop may not cause resetting of origin
        self.post_stops_from_file('TestTAMission.data/step_10_7c.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(len(mission_44.stops), 5)
        self.assertEqual(mission_44.origin_id, 'nl.ah')
        self.assertEqual(mission_44.destination_id, 'nl.asd')

        # STEP 7d: update Utrecht (should already be there)
        self.post_stops_from_file('TestTAMission.data/step_10_7d.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44.origin_id, 'nl.ah')
        self.assertEqual(mission_44.destination_id, 'nl.asd')

        self.assertEqual(len(mission_44.stops), 5)
        self.assertEqual(mission_44.stops[3].station_id, 'nl.ut')
        self.assertEqual(mission_44.stops[3].status, StopStatuses.announced)

        # STEP 7e: update Amsterdam (should already be there)
        self.post_stops_from_file('TestTAMission.data/step_10_7e.json')
        mission_44 = TAMission.get('nl.3044')

        # FRS 10.7.7 Mission must insert new stops after current stops
        self.assertEqual(len(mission_44.stops), 8)
        self.assertEqual(mission_44.stops[4].station_id, 'nl.asd')
        self.assertEqual(mission_44.stops[6].station_id, 'nl.zd')
        self.assertEqual(mission_44.stops[7].station_id, 'nl.amr')

        #FRS 10.7.8 Satus of last_stop must be finalDestination
        self.assertEqual(mission_44.stops[4].status, StopStatuses.announced)
        self.assertEqual(mission_44.stops[6].status, StopStatuses.planned)
        self.assertEqual(mission_44.stops[7].status, StopStatuses.finalDestination)
        self.assertEqual(mission_44.odIDs, ['nl.ah', 'nl.amr'])

        # STEP 7f: insert Amstel (intermediate station, not in series)
        self.post_stops_from_file('TestTAMission.data/step_10_7f.json')
        mission_44 = TAMission.get('nl.3044')

        # FRS 10.7.9 Mission must receive intermediate stop
        self.assertEqual(len(mission_44.stops), 9)
        self.assertEqual(mission_44.stops[4].station_id, 'nl.asa')
        self.assertEqual(mission_44.odIDs, ['nl.ah', 'nl.amr'])

        # STEP 7g: insert Nijmegen (starting station, not in series)
        self.post_stops_from_file('TestTAMission.data/step_10_7g.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44.odIDs, ['nl.ah', 'nl.amr'])

        # FRS 10.7.10 Mission must receive new starting station
        self.assertEqual(len(mission_44.stops), 10)
        self.assertEqual(mission_44.stops[0].station_id, 'nl.nm')

        # STEP 7h: update Alkmaar (destination of series, but indicating a further destination)
        self.post_stops_from_file('TestTAMission.data/step_10_7h.json')
        mission_44 = TAMission.get('nl.3044')

        # FRS 10.7.11 Mission must receive finalDestination outside of series
        self.assertEqual(len(mission_44.stops), 10)
        self.assertEqual(mission_44.stops[9].station_id, 'nl.amr')
        self.assertEqual(mission_44.stops[9].status, StopStatuses.announced)
        self.assertEqual(mission_44.odIDs, ['nl.ah', 'nl.amr'])

        # STEP 7i: insert Schagen (after Alkmaar, not in series)
        self.post_stops_from_file('TestTAMission.data/step_10_7i.json')
        mission_44 = TAMission.get('nl.3044')

        # FRS 10.7.12/13 Mission must receive extra intermediate stop
        self.assertEqual(len(mission_44.stops), 11)
        self.assertEqual(mission_44.stops[10].station_id, 'nl.sgn')
        self.assertEqual(mission_44.stops[10].status, StopStatuses.extra)
        self.assertEqual(mission_44._odIDsDictionary, {'1': ['nl.ah', 'nl.amr'], 'd': [None, None]})

        # STEP 7j: insert Veenendaal De Klomp for the next day
        self.post_stops_from_file('TestTAMission.data/step_10_7j.json')
        mission_44 = TAMission.get('nl.3044')
        self.assertEqual(mission_44.stops[3].station_id, 'nl.klp')
        self.assertEqual(mission_44.stops[3].departure.replace(tzinfo=None), datetime(2013, 2, 19, 13, 46))

        # test counters
        self.assertEqual(read_counter('mission_changes'), 12)
        self.assertEqual(read_counter('mission_small_changes'), 2)
        self.assertEqual(read_counter('mission_no_changes'), 0)

    def test_disruptions(self):
        # Load sample data:
        TSStation.update_stations('TestTAMission.data/stations.xml')
        TASeries.import_xml('TestTAMission.data/series.xml')
        mission = TAMission.get('nl.3046')
        self.assertNotEqual(mission, None)
        mission.activate_mission(mark_cet(datetime(2013, 2, 19, 2)))
        mission.put()
        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        taskq.FlushQueue('default')

        # STEP 8a De Klomp +8
        self.post_stops_from_file('TestTAMission.data/step_10_8a.json')
        mission = TAMission.get('nl.3046')

        self.assertEqual(mission.stops[2].station_id, 'nl.klp')
        self.assertEqual(mission.stops[2].delay_dep, 8.0, "FRS 10.8.1 Stop must receive delay in update")
        self.assertEqual(mission.next_stop_index(mark_cet(datetime(2013, 2, 19, 14, 0))), 0, "FRS 10.8.2 Stop must indicate current position")
        self.assertEqual(mission.next_stop_index(mark_cet(datetime(2013, 2, 19, 14, 20))), 2, "FRS 10.8.2 Stop must indicate current position")
        self.assertEqual(mission.next_stop_index(mark_cet(datetime(2013, 2, 19, 14, 25))), 3, "FRS 10.8.2 Stop must indicate current position")
        self.assertEqual(mission.delay, 0.0, "FRS 10.8.5 Only nextStop must set mission delay")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1,
                         "FRS 10.8.3 After receiving delay mission must queue task for current next stop")
        task0 = tasks[0]
        self.assertEqual(task0['url'], '/agent/station/nl.ah')
        self.assertEqual(task0['name'], '19_1300_41_prio_3046')
        taskq.FlushQueue('default')


        # STEP 8b Arnhem +10
        self.post_stops_from_file('TestTAMission.data/step_10_8b.json')
        mission = TAMission.get('nl.3046')

        self.assertEqual(mission.stops[0].station_id, 'nl.ah')
        self.assertEqual(mission.stops[0].delay_dep, 5.0, "FRS 10.8.1 Stop must receive delay in update")
        self.assertEqual(mission.stops[1].station_id, 'nl.ed')
        self.assertAlmostEqual(mission.stops[1].delay_dep, 4.0, places=1, msg="FRS 10.8.4 Mission must adapt delay in next stops")
        self.assertAlmostEqual(mission.stops[2].delay_dep, 8.0, places=1, msg="FRS 10.8.6 With increasing delay, values may only be increased")
        self.assertEqual(mission.stops[3].station_id, 'nl.ut')
        self.assertAlmostEqual(mission.stops[3].delay_dep, 0.4, places=1, msg="FRS 10.8.4 Mission must adapt delay in next stops")
        self.assertEqual(mission.stops[4].station_id, 'nl.asd')
        self.assertEqual(mission.stops[4].delay_dep, 0.0, "FRS 10.8.1 Stop must receive delay in update")
        self.assertEqual(mission.delay, 5.0, "FRS 10.8.5 nextStop must set mission delay")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 6)
        task0 = tasks[0]
        self.assertEqual(task0['url'], '/agent/station/nl.ed')
        self.assertEqual(task0['name'], '19_1309_xx_prio_3046')
        taskq.FlushQueue('default')

        # STEP 8c Arnhem +15
        self.post_stops_from_file('TestTAMission.data/step_10_8c.json')
        mission = TAMission.get('nl.3046')

        self.assertAlmostEqual(mission.stops[1].delay_dep, 14.0, places=1, msg="FRS 10.8.4 Mission must adapt delay in next stops")
        self.assertAlmostEqual(mission.stops[2].delay_dep, 13.5, places=1, msg="FRS 10.8.4 Mission must adapt delay in next stops")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 5)
        taskq.FlushQueue('default')

        # STEP 8d Arnhem +0
        self.post_stops_from_file('TestTAMission.data/step_10_8d.json')
        mission = TAMission.get('nl.3046')

        for stop in mission.stops:
            self.assertEqual(stop.delay_dep, 0.0)
        self.assertEqual(mission.delay, 0.0, "FRS 10.8.5 nextStop must set mission delay")

        # STEP 9a Zaandam canceled
        self.post_stops_from_file('TestTAMission.data/step_10_9a.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(mission.stops[6].station_id, 'nl.zd')
        self.assertEqual(mission.stops[6].status, StopStatuses.canceled, "FRS 10.9.1 Stop must be marked canceled")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1, "FRS 10.9.2 Must send priority update to neighbour station")
        task0 = tasks[0]
        self.assertEqual(task0['url'], '/agent/station/nl.ass')
        taskq.FlushQueue('default')

        # STEP 9b Zaandam reconfirmed
        self.post_stops_from_file('TestTAMission.data/step_10_9b.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(mission.stops[6].status, StopStatuses.announced, "FRS 10.9.3 Stop must be marked announced")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 2, "FRS 10.9.4 Must send priority update to neighbour station")

        task0 = tasks[0]
        self.assertEqual(task0['url'], '/agent/station/nl.asd')
        task1 = tasks[1]
        self.assertEqual(task1['url'], '/agent/station/nl.ass')
        taskq.FlushQueue('default')

        # STEP 9c altDestination for orphaned mission and non-existing altDestination (may not cause errors)
        self.post_stops_from_file('TestTAMission.data/step_10_9c.json')

        # STEP 9d Utrecht says Amsterdam is altDestination
        self.post_stops_from_file('TestTAMission.data/step_10_9d.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(mission.stops[4].status, StopStatuses.altDestination, "FRS 10.9.5 Stop must be shortened")
        for index in range(5, 8):
            self.assertEqual(mission.stops[index].status, StopStatuses.canceled, "FRS 10.9.5 Stop must be shortened")

        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 1, "FRS 10.9.6 Must send priority update to altered Destination")
        task0 = tasks[0]
        self.assertEqual(task0['url'], '/agent/station/nl.asd')

        # STEP 9e Amsterdam says Alkmaar is finalDestination > FRS 10.9.7
        self.post_stops_from_file('TestTAMission.data/step_10_9e.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(mission.stops[4].status, StopStatuses.announced)
        self.assertEqual(mission.stops[5].status, StopStatuses.planned)
        self.assertEqual(mission.stops[6].status, StopStatuses.planned)
        self.assertEqual(mission.stops[7].status, StopStatuses.finalDestination)

        # STEP 9f
        self.post_stops_from_file('TestTAMission.data/step_10_9f.json')
        mission = TAMission.get('nl.3046')
        self.log_mission(mission)
        mission_s = TAMission.get('nl.303046')
        self.log_mission(mission_s)

        # STEP 9g
        self.post_stops_from_file('TestTAMission.data/step_10_9g.json')
        mission = TAMission.get('nl.3046')
        self.log_mission(mission)
        mission_s = TAMission.get('nl.303046')
        self.log_mission(mission_s)

        # test counters
        self.assertEqual(read_counter('mission_changes'), 15)
        self.assertEqual(read_counter('mission_small_changes'), 0)
        self.assertEqual(read_counter('mission_no_changes'), 0)

    def test_revoke_stops(self):
        """
        FRS 10.10 Revoking stops

        """
        # Load sample data:
        TSStation.update_stations('TestTAMission.data/stations.xml')
        TASeries.import_xml('TestTAMission.data/series.xml')

        mission = TAMission.get('nl.3046')
        mission.activate_mission(mark_cet(datetime(2013, 2, 18, 2)))
        self.assertEqual(len(mission.stops), 5)
        self.assertEqual(mission.origin_id, 'nl.ah')
        self.assertEqual(mission.destination_id, 'nl.asd')
        mission.put()

        # STEP 10a revoke Ede
        self.post_stops_from_file('TestTAMission.data/step_10_10a.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(len(mission.stops), 4)
        self.assertEqual(mission.origin_id, 'nl.ah')
        self.assertEqual(mission.destination_id, 'nl.asd')

        # ...and set a delay for Arnhem - this must not cause an error when revoking Arnhem
        self.assertEqual(mission.first_stop.station_id, 'nl.ah')
        self.assertEqual(mission.first_stop.delay_dep, 15.0)

        # STEP 10b revoke Arnhem
        self.post_stops_from_file('TestTAMission.data/step_10_10b.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(len(mission.stops), 3)
        self.assertEqual(mission.origin_id, 'nl.klp')

        # STEP 10c revoke Utrecht and Veenendaal De Klomp
        self.post_stops_from_file('TestTAMission.data/step_10_10c.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(len(mission.stops), 0)
        self.assertEqual(mission.origin_id, None)
        self.assertEqual(mission.destination_id, None)

        mission = TAMission.get('nl.3046')
        mission.activate_mission(mark_cet(datetime(2013, 2, 19, 2)))
        self.assertEqual(len(mission.stops), 8)
        self.assertEqual(mission.origin_id, 'nl.ah')
        self.assertEqual(mission.destination_id, 'nl.amr')
        mission.put()

        # STEP 10d reset Arnhem
        self.post_stops_from_file('TestTAMission.data/step_10_10d.json')
        mission = TAMission.get('nl.3046')
        self.assertEqual(len(mission.stops), 5)
        self.assertEqual(mission.origin_id, 'nl.ah')
        self.assertEqual(mission.destination_id, 'nl.asd')
        self.assertEqual(mission.stops[4].status, StopStatuses.finalDestination)

    def post_stops_from_file(self, filename):
        stops_file = open(filename, 'r')
        array = json.load(stops_file)
        for element in array:
            url = str(element['url'])
            for payload in element['payload']:
                self.missionApp.post(url, json.dumps(payload), [('Content-Type', 'application/json')])

    def log_mission(self, mission):
        logging.info('Mission %s (%s)' % (mission.id, mission.offset_string))
        for stop in mission.stops:
            logging.info('    %d +%.1f %s %s (naar %s)' % (stop.status, stop.delay_dep, stop.departure_string, stop.station.name, stop.real_destination))

