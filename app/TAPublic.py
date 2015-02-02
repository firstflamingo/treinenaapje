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
#  TAPublic.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 11-Apr-13.
#

import webapp2, logging, re, json
from google.appengine.api import users

from datetime           import timedelta

from ffe.gae            import increase_counter
from ffe.ffe_time       import now_cet, utc_from_cet, cet_from_string
from TASeries           import TASeries
from TAMission          import TAMission
from TAScheduledPoint   import TAScheduledPoint, Direction

# WSGI Handler classes

class TAPublicHandler(webapp2.RequestHandler):

    def validateID(self, string):
        if len(string) <= 9:
            if re.match(r'[a-z]{2}\.', string):
                return True
        self.reject()
        return False
    
    def validateDatetime(self, string):
        if len(string) == 19:
            if re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}', string):
                return True
        self.reject()
        return False
    
    def validateDigit(self, string):
        if len(string) == 1:
            if re.match(r'[0-9]', string):
                return True
        self.reject()
        return False
    
    def reject(self):
        self.error(404)
        self.response.out.write('''
            <html>
            <head><title>404 Not Found</title></head>
            <body>
            <h1>404 Not Found</h1>
            The resource could not be found.<br /><br />
            </body>
            </html>
            ''')


class TrajectoryHandler(TAPublicHandler):
        
    def get(self):
        increase_counter('req_trajectory')
        origin_id = self.request.get('from')
        if not self.validateID(origin_id): return
        destination_id = self.request.get('to')
        if not self.validateID(destination_id): return

        time_string = self.request.get('start', None)
        if time_string is None:
            start_time = now_cet() - timedelta(hours=1)
        else:
            if not self.validateDatetime(time_string): return
            start_time = cet_from_string(time_string)

        span_string = self.request.get('span', None)
        if span_string is None:
            time_span = timedelta(hours=3)
        else:
            if not self.validateDigit(span_string): return
            time_span = timedelta(hours=int(span_string))

        output_string = json.dumps(self.trajectory_dict(origin_id, destination_id, start_time, time_span))
        self.response.out.write(output_string)
        
    @staticmethod
    def trajectory_dict(origin_id, destination_id, start_time, time_span):
        series_origin = set(TAScheduledPoint.series_ids_at_station(origin_id))
        series_destination = set(TAScheduledPoint.series_ids_at_station(destination_id))
        intersection = series_origin & series_destination
        mission_list = []
        for seriesID in intersection:
            series = TASeries.get(seriesID)
            mission_list += series.relevant_mission_tuples(origin_id, start_time, time_span, destinationID=destination_id)
        mission_list.sort()
        
        array = []
        for departure, mission_id in mission_list:
            dictionary = {'v': departure.strftime('%Y-%m-%dT%H:%M:%S'), 'id': mission_id}
            array.append(dictionary)
        
        return {'origin':origin_id, 'destination':destination_id, 'options':array}


class MissionHandler(TAPublicHandler):

    def get(self):
        increase_counter('req_mission')
        mission_id = self.request.get('id')
        if not self.validateID(mission_id): return
    
        mission = TAMission.get(mission_id)
        if not mission:
            self.reject()
            return
        output_string = json.dumps(mission.repr)
        self.response.out.write(output_string)


class DeparturesHandler(TAPublicHandler):

    def get(self):
        increase_counter('req_departures')
        series_id = self.request.get('series')
        if not self.validateID(series_id):
            self.reject()
            return
        origin_id = self.request.get('from')
        if not self.validateID(origin_id):
            self.reject()
            return
        
        direction_string = self.request.get('dir')
        if direction_string == 'up':
            direction = Direction.up
        elif direction_string == 'down':
            direction = Direction.down
        else:
            self.reject()
            return
    
        time_string = self.request.get('start', None)
        if time_string is None:
            start_time = now_cet() - timedelta(hours=1)
        else:
            if not self.validateDatetime(time_string): return
            start_time = cet_from_string(time_string)
        
        span_string = self.request.get('span', None)
        if span_string is None:
            time_span = timedelta(hours=3)
        else:
            if not self.validateDigit(span_string): return
            time_span = timedelta(hours=int(span_string))
        
        series = TASeries.get(series_id)
        if not series:
            self.reject()
            return
        output_string = json.dumps(self.departures_dict(series, origin_id, direction, start_time, time_span))
        self.response.out.write(output_string)

    @staticmethod
    def departures_dict(series, origin_id, direction, start_time, time_span):
        mission_list = series.relevant_mission_tuples(origin_id, start_time, time_span, direction=direction)
        
        array = []
        for departure, missionID in mission_list:
            dictionary = {'v': departure.strftime('%Y-%m-%dT%H:%M:%S'), 'id': missionID}
            array.append(dictionary)
        
        return {'origin': origin_id, 'options': array}

class StatisticsHandler(TAPublicHandler):

    def get(self):
        user = users.get_current_user()
        if user:
            logging.info('stats request by logged in user: %s' % user.nickname())
        else:
            logging.info('stats request by anonymous user')
        now_string = self.request.get('now')
        if now_string:
            now = cet_from_string(now_string)
        else:
            now = now_cet()
        self.response.out.write(json.dumps(TASeries.statistics(now)))


# WSGI Application

URL_SCHEMA = [('/trajectory.*', TrajectoryHandler),
              ('/mission.*', MissionHandler),
              ('/departures.*', DeparturesHandler),
              ('/statistics', StatisticsHandler)]
app = webapp2.WSGIApplication(URL_SCHEMA)
