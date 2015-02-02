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
#  TestTASeries.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 12-Jan-13.
#

"""TestTASeries.py contains a series of tests for TASeries"""

import logging, unittest, json
import webapp2, webtest

from datetime               import timedelta, datetime, time
from google.appengine.api   import memcache, taskqueue
from google.appengine.ext   import db, testbed

from ffe.ffe_time       import mark_cet
from TAScheduledPoint   import TAScheduledPoint, Direction
from TASeries           import TASeries, app
from TSStation          import TSStation
from TAMission          import TAMission
from TAStop             import TAStop
from TAChart            import TAChart

class TestTASeries(unittest.TestCase):
    
    def setUp(self):
        self.seriesApp = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()
        
        logger = logging.getLogger()
        logger.level = logging.DEBUG
    
    def tearDown(self):
        self.testbed.deactivate()
    
    def test_import_export(self):
        """
        FRS 9.2 Importing and exporting a series

        """
        TASeries.import_xml('TestTASeries.data/series_basic.xml')
        
        # Supplementary missions must not appear in xml output
        mission = TAMission.get('nl.300546', create=True)
        mission.put()
        series = TASeries.get('nl.005')
        self.assertEqual(series.name, '500',
                         "FRS 9.1.1 A TASeries name property must provide the series number in hundreds")

        # FRS 9.3.1 A TASeries object must provide its contents in xml-format.
        input_xml = open('TestTASeries.data/series_basic.xml', 'r').read()
        output_xml = series.xml_document.write(lf=True)
        self.assertEqual(input_xml.strip(), output_xml.strip())

    def test_accessing_points(self):
        TASeries.import_xml('TestTASeries.data/series_313.xml')
        series = TASeries.get('nl.313')
        
        self.assertEqual(len(series.points), 7,
                         "FRS 9.4.1 TASeries must provide a list with scheduledPoints")
        self.assertEqual(series.first_point.station_id, 'nl.amf',
                         "FRS 9.4.2 TASeries must provide a shortcut for the first scheduledPoint")
        self.assertEqual(series.last_point.station_id, 'nl.ed',
                         "FRS 9.4.2 TASeries must provide a shortcut for the last scheduledPoint")
        point_bnc = series.point_at_index(3)
        self.assertEqual(point_bnc.station_id, 'nl.bnc',
                         "FRS 9.4.3 TASeries must provide the scheduledPoint on a given index")
        self.assertEqual(series.index_for_station('nl.bnc'), 3,
                         "FRS 9.4.4 TASeries must provide the index where a station can be found in the list of points")
        point_bnc = series.point_for_station('nl.bnc')
        self.assertEqual(point_bnc.station_id, 'nl.bnc',
                         "FRS 9.4.5 TASeries must provide the scheduledPoint for a station by name or id")
        point_bnc = series.point_for_station('Barneveld Centrum')
        self.assertEqual(point_bnc.station_id, 'nl.bnc',
                         "FRS 9.4.5 TASeries must provide the scheduledPoint for a station by name or id")
        self.assertEqual(len(series.points_in_range('nl.amf', 'nl.bnc')), 4,
                         "FRS 9.4.6 TASeries must provide a list of points between an origin and a destination")
        self.assertEqual(series.point_at_index(None), None,
                         "FRS 9.4.7 TASeries must return None when a point or index cannot be found")
        self.assertEqual(series.index_for_station('nonExistant'), None,
                         "FRS 9.4.7 TASeries must return None when a point or index cannot be found")
    
    def test_new_day(self):
        """
        FRS 9.5 Activating new day
        FRS 9.8 Providing statistics
        """
        TSStation.update_stations('TestTASeries.data/stations_020.xml')
        TASeries.import_xml('TestTASeries.data/series_020.xml')
        
        supplementary_mission = TAMission.new('nl.302020')
        supplementary_mission.offset_cet = datetime(2013, 5, 17, 8, 0)
        series = TASeries.get('nl.020')
        series.add_mission(supplementary_mission)
        self.assertEqual(series.nr_of_missions, 14,
                         "A supplementary mission must be added to the series")
    
        response = self.seriesApp.post('/TASeries/nl.020', {'inst': 'new_day', 'now': '2013-05-18T02:00:00+0100'})
        self.assertEqual(response.status, '200 OK')
        series = TASeries.get('nl.020')
        self.assertEqual(series.nr_of_missions, 13,
                         "FRS 9.5.2 Supplementary missions must be removed")
        
        mission_2024 = TAMission.get('nl.2024')
        self.assertEqual(len(mission_2024.stops), 3,
                         "FRS 9.5.3 Stops for the next day must be added to the mission")
        
        point_gd = TAScheduledPoint.get('nl.020_gd')
        self.assertEqual(point_gd.scheduled_times, (26, 27, 33, 34))
        self.assertEqual(point_gd.platform_list ,[['10'], ['5']])
    
        self.update_stops_from_file('TestTASeries.data/stops_020.json')
        
        # FRS 9.8.1 TASeries must provide statistics
        result = TASeries.statistics(mark_cet(datetime(2013, 5, 18, 8, 30)))
        expected = {'status': {'running': 2},
                    'delay': {'0': 1, '2': 1},
                    'counter': {'mission_changes': 13, 'mission_no_changes': 0, 'mission_small_changes': 0,
                                'req_api_success': 0, 'req_api_total': 0, 'req_avt_answered': 0,
                                'req_avt_denied': 0, 'req_check_confirmed': 0, 'req_check_denied': 0,
                                'req_check_refetched': 0,
                                'req_check_revoked': 0, 'req_departures': 0, 'req_mission': 0,
                                'req_prio_answered': 0, 'req_prio_denied': 0, 'req_trajectory': 0}}
        self.assertEqual(expected, result)

        response = self.seriesApp.post('/TASeries/nl.020', {'inst': 'new_day', 'now': '2013-05-19T02:00:00+0100'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(series.nr_of_missions, 13)

        chart = TAChart.get('nl.020_201320')
        expected = {
            'pattern_up': {'nl.gvc': {'9': 2}, 'nl.gd': {'30': 2}, 'nl.ut': {'46': 2}},
            'pattern_down': {'nl.gd': {'35': 11}, 'nl.gvc': {'52': 11}, 'nl.ut': {'14': 11}},
            'delay_up': {'nl.gvc': {'0.0': 2}, 'nl.gd': {'2.0': 2}, 'nl.ut': {'0.0': 2}},
            'delay_down': {'nl.gd': {'0.0': 8, '2.0': 3}, 'nl.gvc': {'0.0': 11}, 'nl.ut': {'0.0': 11}},
            'platform_up': {'nl.gvc': {'5': 2}, 'nl.gd': {'4': 2}, 'nl.ut': {}},
            'platform_down': {'nl.gd': {'8': 10, '10': 1}, 'nl.gvc': {}, 'nl.ut': {'9': 11}}}
        result = chart._dataDictionary
        self.assertEqual(expected, result,
                         "FRS 9.5.1 Data over the past day must be processed in a chart.\nExpected: %s\nResult:   %s"
                         % (expected, result))

        mission_2024 = TAMission.get('nl.2024')
        self.assertEqual(len(mission_2024.stops), 0,
                         "FRS 9.5.3 Stops for the next day must be added to the mission")

        point_gd = TAScheduledPoint.get('nl.020_gd')
        self.assertEqual(point_gd.scheduled_times, (26, 27, 34, 35),
                         "FRS 9.5.4 TAChart must verify points and modify when needed.")
        self.assertEqual(point_gd.platform_list ,[['8'], ['4']],
                         "FRS 9.5.4 TAChart must verify points and modify when needed.")

        # FRS 9.9 TASeries must delete references to a station
        taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        taskq.FlushQueue('default')
        series = TASeries.get('nl.020')
        self.assertEqual(len(series.points), 3)

        response = self.seriesApp.post('/TASeries/nl.020', {'inst': 'delete_point', 'sender': 'nl.gd'})
        self.assertEqual(response.status, '200 OK')
        series = TASeries.get('nl.020')
        self.assertEqual(len(series.points), 2)
        tasks = taskq.GetTasks('default')
        self.assertEqual(len(tasks), 13)

    def test_mission_management(self):
        TASeries.import_xml('TestTASeries.data/series_005.xml')
        series = TASeries.get('nl.005')
        
        self.assertEqual(series.nr_of_missions, 12,
                         "FRS 9.6.1 TASeries must show how much missions it contains")
        mission = TAMission.get('nl.300527', create=True)
        mission.put()
        series = TASeries.get('nl.005')
        self.assertEqual(series.nr_of_missions, 13,
                         "FRS 9.6.2 TASeries must make it possible to add a new mission")
        expected = ['nl.523', 'nl.527', 'nl.300527', 'nl.531', 'nl.535', 'nl.539', 'nl.543']
        result = series.all_mission_ids(Direction.up)
        self.assertEqual(expected, result,
                         "FRS 9.6.3 TASeries must provide all missions belonging to a series.\nExpected: %s\nResult:   %s"
                         % (expected, result))
        now = datetime(2011, 1, 11, 11, 11)
        expected = ['nl.527', 'nl.300527', 'nl.531', 'nl.535', 'nl.539']
        result = series.current_mission_ids(Direction.up, now)
        self.assertEqual(expected, result,
                         "FRS 9.6.4 TASeries must provide its current missions.\nExpected: %s\nResult:   %s"
                         % (expected, result))
        expected = ['nl.528', 'nl.532', 'nl.536']
        result = series.current_mission_ids(Direction.down, now)
        self.assertEqual(expected, result,
                         "FRS 9.6.4 TASeries must provide its current missions.\nExpected: %s\nResult:   %s"
                         % (expected, result))
        searchStart = datetime(2011, 1, 11, 8, 8)
        searchSpan = timedelta(hours=2)
        expected = [(datetime(2011, 1, 11, 8, 50), 'nl.527'),
                    (datetime(2011, 1, 11, 8, 50), 'nl.300527'),
                    (datetime(2011, 1, 11, 9, 50), 'nl.531')]
        result = series.relevant_mission_tuples('nl.ut', searchStart, searchSpan, destinationID='nl.zl')
        self.assertEqual(expected, result,
                         "FRS 9.6.5 TASeries must provide a list of relevant missions.\nExpected: %s\nResult:   %s"
                         % (expected, result))
        expected = [(datetime(2011, 1, 11, 8, 18), 'nl.524'),
                    (datetime(2011, 1, 11, 9, 18), 'nl.528')]
        result = series.relevant_mission_tuples('nl.zl', searchStart, searchSpan, destinationID='nl.ut')
        self.assertEqual(expected, result,
                         "FRS 9.6.5 TASeries must provide a list of relevant missions.\nExpected: %s\nResult:   %s"
                         % (expected, result))

    def test_offset_management(self):
        TASeries.import_xml('TestTASeries.data/series_370.xml')
        series = TASeries.get('nl.370')
    
        expected = [{28: 13, 37: 1, 7: 1},
                    {43: 12, 46: 1, 23: 2},
                    {32: 1, 28: 1, 7: 13},
                    {3: 14}]
        result = series.offset_overview
        self.assertEqual(expected, result,
                         "FRS 9.7.1 TASeries must provide an overview with frequency of offset times.\nExpected: %s\nResult:   %s"
                         % (expected, result))
    
        deltaOffsets = series.needed_offset_changes
        self.assertEqual(deltaOffsets, [7, 3])
        series.change_offsets(deltaOffsets)
        self.assertEqual(series.first_point.scheduled_times, (4, 4, 42, 42))
        self.assertEqual(series.last_point.scheduled_times, (23, 23, 22, 22))
    
        expected = [{21: 13, 30: 1, 0: 1},
                    {40: 12, 43: 1, 20: 2},
                    {25: 1, 21: 1, 0: 13},
                    {0: 14}]
        result = series.offset_overview
        self.assertEqual(expected, result,
                         "FRS 9.7.2 TASeries must be able to change offset times.\nExpected: %s\nResult:   %s"
                         % (expected, result))

    def update_stops_from_file(self, filename):
        stops_file = open(filename, 'r')
        array = json.load(stops_file)
        for element in array:
            mission = TAMission.get(str(element['id']))
            stop = TAStop.fromRepr(element['payload'])
            mission.update_stop(stop)

