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
#  TASeries.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 21-Feb-13.
#

import webapp2, logging, xml.sax, bisect

from google.appengine.ext import db
from google.appengine.api import memcache
from datetime import timedelta, datetime

from ffe                import config
from ffe.gae            import counter_dict, issue_tasks
from ffe.markup         import XMLDocument, XMLElement
from ffe.ffe_time       import now_utc, now_cet, mark_utc, minutes_from_string, cet_from_string, minutes_from_time, time_from_minutes
from TABasics           import TAModel, TAResourceHandler
from TAScheduledPoint   import TAScheduledPoint, Direction
from TAMission          import TAMission, MissionStatuses, round_mission_offset
from TSStation          import TSStation
from TAStop             import TAStop
from TAChart            import TAChart


# ====== Series Model ==========================================================================

class TASeries(TAModel):
    agent_url = '/TASeries'

    # Stored attributes:
    type = db.StringProperty(indexed=False)

    # Transient attributes
    _points                 = None
    _points_dict            = None
    _missions_list          = None

    @classmethod
    def import_xml(cls, filename):
        fp = open(filename, 'r')
        xml.sax.parse(fp, SeriesImporter())

    def import_schedule(self):
        filename = 'series.data/%s.xml' % self.id
        logging.info('import %s' % filename)
        xml_string = open(filename, 'r').read()
        TAScheduledPoint.parse_schedule(xml_string, self)
    
    @classmethod
    def statistics(cls, now=None):
        if now is None:
            now = now_cet()
        status_hist = {}
        delay_hist = {}
        for seriesID in TASeries.all_ids():
            series = TASeries.get(seriesID)
            for mission_id in series.current_mission_ids(Direction.up, now) + series.current_mission_ids(Direction.down, now):
                mission = TAMission.get(mission_id)
                status, delay = mission.status_at_time(now)
                data = MissionStatuses.s[status]
                status_hist[data] = status_hist.get(data, 0) + 1
                if status == MissionStatuses.running:
                    data = '%.0f' % delay
                    delay_hist[data] = delay_hist.get(data, 0) + 1
        return {'status': status_hist, 'delay': delay_hist, 'counter': counter_dict()}
    
    @property
    def name(self):
        if self.country == 'eu':
            return self.code
        else:
            return '%d00' % int(self.code)

    def load_points(self):
        query = db.Query(TAScheduledPoint).filter('series_id =', self.id).order('km')
        array = query.fetch(100)
        if not array:
            array = []
        self._points = array
        
        self._points_dict = {}
        for index in range(len(self._points)):
            point = self._points[index]
            self._points_dict[point.station_id] = index
            name = point.stationName
            if name:
                self._points_dict[name] = index
        self.cache_set()

    def reset_points(self):
        self._points = None
        self._points_dict = None
        self.cache_set()

    @property
    def points(self):
        if self._points is None:
            self.load_points()
        return self._points

    @property
    def points_dict(self):
        if self._points_dict is None:
            self.load_points()
        return self._points_dict

    @property
    def first_point(self):
        if self.points:
            return self.points[0]

    @property
    def last_point(self):
        if self.points:
            return self.points[-1]

    @property
    def origin(self):
        return self.name_for_point(self.first_point)

    @property
    def destination(self):
        return self.name_for_point(self.last_point)

    @staticmethod
    def name_for_point(point):
        if point is None:
            return '-'
        else:
            station_name = point.stationName
            if station_name is None:
                station = TAStation.get(point.station_id)
                station_name = station.name
                point.stationName = station_name
                point.put()
            return station_name

    @property
    def mission_lists(self):
        if self._missions_list is None:
            down_array = []
            up_array = []
            query = db.Query(TAMission).filter('series_id =', self.id)
            for mission in query.fetch(200):
                if mission.up:
                    up_array.append((mission.offset_time, mission.number))
                else:
                    down_array.append((mission.offset_time, mission.number))
            up_array.sort()
            down_array.sort()
            self.mission_lists = [down_array, up_array]
        return self._missions_list

    @mission_lists.setter
    def mission_lists(self, array):
        self._missions_list = array
        self.cache_set()

    @property
    def planned_mission_ids(self):
        array = []
        for direction in (Direction.up, Direction.down):
            for offset, number in self.mission_lists[direction]:
                if number < 99999:
                    array.append('%s.%d' % (self.country, number))
        return array

    @property
    def nr_of_missions(self):
        return len(self.mission_lists[Direction.down]) + len(self.mission_lists[Direction.up])

    def get_missions(self, direction, current=False):
        array = []
        if current:
            id_list = self.current_mission_ids(direction)
        else:
            id_list = self.all_mission_ids(direction)
        for missionID in id_list:
            mission = TAMission.get(missionID)
            if not mission:
                continue
            array.append(mission)
        return array

    @property
    def up_missions(self):
        return self.get_missions(Direction.up, current=False)

    @property
    def down_missions(self):
        return self.get_missions(Direction.down, current=False)

    @property
    def current_up_missions(self):
        return self.get_missions(Direction.up, current=True)

    @property
    def current_down_missions(self):
        return self.get_missions(Direction.down, current=True)

    @property
    def offset_overview(self):
        overview = [{}, {}, {}, {}]
        for direction in (Direction.up, Direction.down):
            for offset, number in self.mission_lists[direction]:
                histogram = overview[number % 4]
                key = offset.minute
                histogram[key] = histogram.get(key, 0) + 1
        return overview

    @property
    def needed_offset_changes(self):
        delta_offsets = [None, None]
        overview = self.offset_overview
        for group in range(4):
            max_frequency = 0
            offset = 0
            for key, value in overview[group].iteritems():
                if value > max_frequency:
                    max_frequency = value
                    offset = int(key)
            direction = group % 2
            if direction == Direction.up:
                departure = offset + self.first_point.upDeparture
            else:
                departure = offset + self.last_point.downDeparture
            if departure >= 60:
                offset -= 60
            if delta_offsets[direction] is None or offset < delta_offsets[direction]:
                delta_offsets[direction] = offset
        return delta_offsets

    @property
    def xml(self):
        element = XMLElement('series', {'id': self.id, 'type': self.type})
        return element

    @property
    def xml_schedule(self):
        element = self.xml
        points_tag = XMLElement('routePoints')
        up_tag = XMLElement('upSchedule')
        down_tag = XMLElement('downSchedule')
        for point in self.points:
            points_tag.add(point.station_xml)
            up_tag.add(point.up_xml)
            down_tag.add(point.down_xml)
        element.add(points_tag)
        element.add(up_tag)
        element.add(down_tag)
        return element

    @property
    def xml_missions(self):
        element = self.xml
        missions_tag = XMLElement('missions')
        for missionID in self.planned_mission_ids:
            mission = TAMission.get(missionID)
            missions_tag.add(mission.xml)
        element.add(missions_tag)
        return element

    @property
    def xml_document(self):
        document = TimetableDocument()
        document.root.add(self.xml_schedule)
        return document

    def activate_new_day(self, now):
        year, week, iso_day = now.isocalendar()
        chart_id = '%s_%04d%02d' % (self.id, year, week)
        chart = self.get_chart_with_id(chart_id)
        
        new_missions_list = [[], []]
        updated_missions = {}
        expired_mission_ids = []
        expired_missions = []
        updated_points = {}
        updated_objects = [chart]
        
        for mission in (self.down_missions + self.up_missions):
            chart.add_mission(mission)
            if mission.supplementary:
                expired_mission_ids.append(mission.id)
                expired_missions.append(mission)
            else:
                mission.stops = []
                mission.nominalDate = now.date()
                mission.activate_mission(now)
                new_missions_list[mission.up].append((mission.offset_time, mission.number))
                updated_missions[mission.id] = mission
                updated_objects.append(mission)
    
        if iso_day == 7:
            for point in self.points:
                chart.verifyPoint(point)
                if point.needs_datastore_put:
                    updated_points[point.id] = point
                    updated_objects.append(point)

        self.mission_lists = new_missions_list
        self.cache_set()
        chart.cache_set()
        memcache.delete_multi(expired_mission_ids, namespace='TAMission')
        memcache.set_multi(updated_missions, namespace='TAMission')
        memcache.set_multi(updated_points, namespace='TAScheduledPoint')
        db.delete(expired_missions)
        db.put(updated_objects)

    @staticmethod
    def get_chart_with_id(identifier):
        chart = TAChart.get(identifier)
        if not chart:
            chart = TAChart.new(identifier)
        return chart
    
    # Managing RoutePoints:
    def point_at_index(self, index):
        if index is not None and len(self.points) > index:
            return self.points[index]
        else:
            logging.warning('series %s tries retrieving None index' % self.id)
            return None

    def index_for_station(self, name_or_id):
        index = self.points_dict.get(name_or_id, None)
        if index is None:
            identifier = TSStation.id_for_name(name_or_id)
            if identifier:
                index = self.points_dict.get(identifier, None)
                if index is not None:
                    point = self.points[index]
                    point.stationName = name_or_id
                    point.put()
                    self._points_dict = None
            if index is None:
                logging.info('station %s not found in series %s' % (name_or_id, self.id))
        return index

    def point_for_station(self, name_or_id):
        index = self.index_for_station(name_or_id)
        if index is not None:
            return self.points[index]

    def points_in_range(self, from_station, to_station):
        from_index = self.index_for_station(from_station)
        to_index = self.index_for_station(to_station)
        result = []
        if from_index is not None and to_index is not None:
            start = min(from_index, to_index)
            stop = max(from_index, to_index) + 1
            for index in range(start, stop):
                result.append(self.points[index])
            if from_index > to_index:
                result.reverse()
        return result

    def delete_point(self, station_id):
        expired_point = self.point_for_station(station_id)
        if expired_point:
            logging.info('Delete point %s' % station_id)
            tasks = []
            issue_time_cet = now_cet()
            for mission_id in self.all_mission_ids(Direction.up) + self.all_mission_ids(Direction.down):
                issue_time_cet += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
                tasks.append(self.stop_task(TAStop.revoked_stop(mission_id, station_id), issue_time_cet))
            issue_tasks(tasks)
            expired_point.delete()
            self.reset_points()
        else:
            logging.warning('Point %s could not be found for deletion' % station_id)

    # Managing missions
    def add_mission(self, mission):
        if mission.up:
            array = self.mission_lists[Direction.up]
        else:
            array = self.mission_lists[Direction.down]
        mission_tuple = (mission.offset_time, mission.number)
        if not mission_tuple in array:
            bisect.insort(array, mission_tuple)
            self.cache_set()

    def all_mission_ids(self, direction):
        array = []
        for offset, number in self.mission_lists[direction]:
            array.append('%s.%d' % (self.country, number))
        return array

    def current_mission_ids(self, direction, now=None):
        if now is None:
            now = now_cet()
        if direction == Direction.up:
            start_time = now - timedelta(minutes=(self.last_point.upArrival + 30))
            end_time = now - timedelta(minutes=self.first_point.upDeparture)
        else:
            start_time = now - timedelta(minutes=(self.first_point.downArrival + 30))
            end_time = now - timedelta(minutes=self.last_point.downDeparture)
        min_time = (now - timedelta(hours=3)).replace(hour=0, minute=0, second=0)
        if start_time < min_time: start_time = min_time
        max_time = min_time.replace(hour=23, minute=59, second=59)
        if end_time > max_time: end_time = max_time

        source = self.mission_lists[direction]
        start_index = bisect.bisect_left(source, (start_time.time(), 0))
        end_index = bisect.bisect_right(source, (end_time.time(), 999999), lo=start_index)
        output = []
        for index in range(start_index, end_index):
            offset, number = source[index]
            output.append('%s.%d' % (self.country, number))

        return output

    def relevant_mission_tuples(self, originID, startTime, timeSpan, direction=None, destinationID=None):
        origin_point = self.point_for_station(originID)
        if not origin_point: return None
        
        if direction is None:
            destination_point = self.point_for_station(destinationID)
            if not destination_point: return None
            if origin_point.upDeparture < destination_point.upDeparture:
                direction = Direction.up
            else:
                direction = Direction.down
        
        source = self.mission_lists[direction]
        departure = origin_point.departure_in_direction(direction)
        
        start_minutes = minutes_from_time(startTime) - departure
        end_minutes = start_minutes + (timeSpan.seconds // 60)
        start_search = time_from_minutes(max(0, start_minutes))
        end_search = time_from_minutes(min(1439, end_minutes))
        start_index = bisect.bisect_left(source, (start_search, 0))
        end_index = bisect.bisect_right(source, (end_search, 999999), lo=start_index)

        output = []
        for index in range(start_index, end_index):
            offset, number = source[index]
            base_time = startTime.replace(hour=offset.hour, minute=offset.minute)
            departure_time = base_time + timedelta(minutes=departure)
            mission_id = '%s.%d' % (self.country, number)
            output.append((departure_time, mission_id))
        
        return output

    def change_offsets(self, deltaOffsets):
        new_list = [[], []]
        processed_objects   = []
        processed_missions  = {}
        processed_points    = {}
        
        for point in self.points:
            point.upArrival += deltaOffsets[Direction.up]
            point.upDeparture += deltaOffsets[Direction.up]
            point.downArrival += deltaOffsets[Direction.down]
            point.downDeparture += deltaOffsets[Direction.down]
            processed_points[point.id] = point
            processed_objects.append(point)

        for direction in (Direction.up, Direction.down):
            if deltaOffsets[direction]:
                for missionID in self.all_mission_ids(direction):
                    mission = TAMission.get(missionID)
                    old_offset = datetime(2002, 2, 2).replace(hour=mission.offset_time.hour, minute=mission.offset_time.minute)
                    new_offset = round_mission_offset(old_offset - timedelta(minutes=deltaOffsets[direction]))
                    mission.offset_time = new_offset.time()
                    new_list[direction].append((mission.offset_time, mission.number))
                    processed_missions[missionID] = mission
                    processed_objects.append(mission)

        self._missions_list = new_list
        memcache.set_multi(processed_points, namespace='TAScheduledPoint')
        memcache.set_multi(processed_missions, namespace='TAMission')
        db.put(processed_objects)
        self.cache_set()


# ====== Series Handler ==========================================================================

class TASeriesHandler(TAResourceHandler):
    resourceClass = TASeries
    
    def perform(self):
        instruction = self.request.get('inst')

        if instruction == 'fetch':
            if self.resource:
                self.resource.import_schedule()
                self.response.out.write('<a href=\"/console/series?id=%s\">terug naar serie</a>' % self.resource.id)
            else:
                TASeries.import_xml('series.data/series.xml')
                self.redirect('/console/series')
            return

        if not self.resource:
            logging.warning('Resource not found.')
            return

        if instruction == 'new_day':
            now_string = self.request.get('now')
            if now_string:
                now = cet_from_string(self.request.get('now'))
            else:
                now = now_cet()
            self.resource.activate_new_day(now)

        elif instruction == 'delete_point':
            sender = self.request.get('sender')
            self.resource.delete_point(sender)
            self.response.out.write('<a href=\"/console/series?id=%s\">terug naar serie</a>' % self.resource.id)

        elif instruction == 'optimize_odids':
            changed_missions = []
            for mission in self.resource.up_missions + self.resource.down_missions:
                mission.optimize_odIDs_dictionary()
                if mission.needs_datastore_put:
                    changed_missions.append(mission)
            memcache.set_multi(TAMission.dictionary_from_list(changed_missions), namespace='TAMission')
            db.put(changed_missions)
            self.response.out.write('<a href=\"/console/missions?kind=pattern&series=%s\">terug naar serie</a>' %
                                    self.resource.id)


# ========== Mission Handler =====================================================================

class TAMissionHandler(TAResourceHandler):
    resourceClass = TAMission
    
    def perform(self):
        instruction = self.request.get('inst')
        if instruction == 'check':
            self.resource.check_mission_announcements(now_cet())
    
    def receive(self, dictionary):
        stop = TAStop.fromRepr(dictionary)
        mission = TAMission.get(self.resource_id, create=True)
        mission.update_stop(stop)


# ====== XML Parsers ==========================================================================

class SeriesImporter(xml.sax.handler.ContentHandler):
    series = None
    mission = None
    
    def startElement(self, name, attrs):
        self.data = []
        if name == 'series':
            id = attrs['id']
            logging.info('import series: %s', id)
            self.series = TASeries.new(id)
            self.series.type = attrs.get('type')
            self.routePoints = {}
        
        if self.series:
            if name == 'station':
                point = TAScheduledPoint.new_with(self.series.id, attrs['id'])
                point.km = float(attrs['km'])
                stationName = attrs.get('name', None)
                if stationName: point.stationName = stationName
                self.routePoints[attrs['id']] = point
            
            if name == 'up':
                point = self.routePoints[attrs['station']]
                point.upArrival = minutes_from_string(attrs['arr'])
                point.upDeparture = minutes_from_string(attrs['dep'])
                point.set_platform_string(Direction.up, attrs.get('platform', '-'))
            
            if name == 'down':
                point = self.routePoints[attrs['station']]
                point.downArrival = minutes_from_string(attrs['arr'])
                point.downDeparture = minutes_from_string(attrs['dep'])
                point.set_platform_string(Direction.down, attrs.get('platform', '-'))

            if name == 'mission':
                self.mission = TAMission.new(attrs['id'])
                self.mission.series_id = self.series.id
                self.mission.offset_string = attrs['offset']

            if self.mission:
                if name == 'od':
                    origin = attrs['from']
                    if origin == 'None': origin = None
                    destination = attrs['to']
                    if destination == 'None': destination = None
                    self.mission.odIDs_dictionary[str(attrs['day'])] = [origin, destination]
    
    def endElement(self, name):
        if name == 'series':
            self.series.put()
            for point in self.routePoints.itervalues():
                point.put()
            self.series = None
            self.routePoints = None
        
        if name == 'mission' and self.mission:
            self.mission.put()

    def characters(self, string):
        self.data.append(string.strip())

# ====== Timetable Document ==========================================================================

class TimetableDocument(XMLDocument):
    
    def __init__(self):
        XMLDocument.__init__(self, 'timetable')

# ====== WSGI Application ==========================================================================

SERIES_URL_SCHEMA = [('/TASeries.*', TASeriesHandler),
                     ('/TAMission.*', TAMissionHandler)]
app = webapp2.WSGIApplication(SERIES_URL_SCHEMA, debug=True)
