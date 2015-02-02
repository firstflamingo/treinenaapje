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
#  TSStationAgent.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 22-May-14.
#

"""TSStationAgent is ..."""

import logging, re
from datetime import timedelta
from google.appengine.api import memcache

from ffe import config
from ffe.gae import increase_counter, remote_fetch, issue_tasks
from ffe.ffe_time import now_cet, cet_from_string
from ffe.rest_resources import NoValidIdentifierError
from TAStop import TAStop


class TSStationAgent(object):
    url_name = 'station'
    id_ = None
    identifier_regex = re.compile('([a-z]{2})\.([a-z]{1,5})$')
    nr_of_fetches = 0
    updated = None
    last_departure = None
    _stops_dictionary = None

    # ------------ Object lifecycle ------------------------------------------------------------------------------------

    def __init__(self, id_):
        self.id_ = id_

    @classmethod
    def get(cls, identifier):
        self = memcache.get(identifier, namespace=cls.__name__)
        if not self:
            self = cls(identifier)
        return self

    def cache_set(self):
        memcache.set(self.id_, self, namespace=self.__class__.__name__)

    # ------------ Object metadata -------------------------------------------------------------------------------------

    @classmethod
    def valid_identifier(cls, identifier):
        if identifier is None or not cls.identifier_regex.match(str(identifier)):
            raise NoValidIdentifierError
        else:
            return identifier

    @property
    def url(self):
        return '/%s/%s' % (self.url_name, self.id_)

    def __repr__(self):
        if self.updated is None:
            time_string = 'empty'
        else:
            time_string = self.updated.strftime('%H:%M:%S')
        return "<%s:%s - %s>" % (self.__class__.__name__, self.id_, time_string)

    @property
    def station_id(self):
        return self.id_

    @property
    def country(self):
        return self.identifier_regex.match(self.id_).group(1)

    @property
    def code(self):
        return self.identifier_regex.match(self.id_).group(2)

    # ------------ Stops management ------------------------------------------------------------------------------------

    @property
    def stops_dictionary(self):
        if self._stops_dictionary is None:
            self._stops_dictionary = {}
        return self._stops_dictionary

    @stops_dictionary.setter
    def stops_dictionary(self, dictionary):
        self._stops_dictionary = dictionary

    @property
    def sorted_stops(self):
        t = []
        for the_stop in self.stops_dictionary.itervalues():
            t.append((the_stop.departure, the_stop))
        t.sort()
        result = []
        for the_time, the_stop in t:
            result.append(the_stop)
        return result

    # ------------ Handling requests -----------------------------------------------------------------------------------

    def execute_request(self, request):
        instruction = request.get('inst')
        now_string = request.get('now', None)
        if now_string:
            now = cet_from_string(now_string)
            test_file = request.get('file', 'ignore')
        else:
            now = now_cet()
            test_file = None

        if instruction == 'avt':
            if self.answer_avt(now):
                increase_counter('req_avt_answered')
                self.perform_avt(now, test_file)
            else:
                increase_counter('req_avt_denied')

        elif instruction == 'prio':
            if self.answer_prio(now):
                increase_counter('req_prio_answered')
                self.perform_avt(now, test_file)
            else:
                increase_counter('req_prio_denied')

        elif instruction == 'check':
            mission_id = request.get('sender')
            exp_s = request.get('expected')
            if exp_s:
                expected = cet_from_string(exp_s)
            else:
                expected = now_cet() + timedelta(days=1)
            self.perform_check(mission_id, now, expected, test_file)

        elif instruction == 'console':
            self.perform_avt(now, test_file)

    def answer_avt(self, now):
        if self.updated is None:
            return True
        if now - self.updated < timedelta(minutes=config.MIN_INTERVAL_BEFORE_AVT_REQ):
            return False
        if self.last_departure is None:
            return True
        if self.last_departure - now > timedelta(minutes=config.MIN_PERIOD_STORED_DEPARTURES):
            return False
        else:
            return True

    def answer_prio(self, now):
        if self.updated is None:
            return True
        if now - self.updated < timedelta(minutes=config.MIN_INTERVAL_BEFORE_PRIO_REQ):
            return False
        else:
            return True

    def perform_avt(self, now, test_file=None):
        self.updated = now
        changed_stops = self.changed_stops(test_file)
        if changed_stops:
            self.forward_changed_stops(changed_stops, now)
        else:
            logging.warning('No stops were fetched')

    def perform_check(self, mission_id, now, expected, test_file=None):
        logging.info('Check stop of mission %s' % mission_id)
        comps = mission_id.split('.')
        stop_code = '%s_%s' % (comps[1], self.code)
        stop = self.stops_dictionary.get(stop_code)
        if stop:
            increase_counter('req_check_confirmed')
            self.forward_changed_stops([stop], now)
        else:
            if self.last_departure is not None and expected < self.last_departure:
                increase_counter('req_check_denied')
                self.forward_changed_stops([TAStop.revoked_stop(mission_id, self.station_id)], now)
            else:
                self.updated = now
                changed_stops = self.changed_stops(test_file)
                if changed_stops is not None:
                    if not self.stops_dictionary.get(stop_code):
                        increase_counter('req_check_revoked')
                        logging.warning('stop was revoked')
                        changed_stops.append(TAStop.revoked_stop(mission_id, self.station_id))
                    else:
                        increase_counter('req_check_refetched')
                    self.forward_changed_stops(changed_stops, now)
                else:
                    logging.warning('No stops were fetched')

    def changed_stops(self, file_name=None):
        """
        Acquires an xml-string with stops, either from NS-API or from the specified file,
        parses the stops, compares them with the current stops and returns a list with changed stops.
        :param file_name: Name of the source ('None' redirects to NS-API; specified in unit-tests)
        :return: A list of TAStop objects
        """
        xml_string = None

        if file_name:
            if file_name != 'ignore':
                fp = open(file_name, 'r')
                xml_string = fp.read()
        else:
            increase_counter('req_api_total')
            url = config.NSAPI_AVT_URL % self.code
            xml_string = remote_fetch(url, headers=config.NSAPI_HEADER, deadline=config.NSAPI_DEADLINE)

        if xml_string:
            return TAStop.parse_avt(xml_string, delegate=self)

    @staticmethod
    def forward_changed_stops(stops, issue_time_cet):
        """
        Forwards stops to their mission in order to notify changes, by creating tasks and issuing them to the taskqueue
        :param stops: A list of TAStop objects
        :param issue_time_cet: The time the tasks must be issued ('now' in production; specified in unit-tests)
        """
        tasks = []
        interval = timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
        for stop in stops:
            issue_time_cet += interval
            tasks.append(stop.forward_to_mission(issue_time_cet))
        issue_tasks(tasks)

