# coding=utf-8
#
#  Copyright (c) 2012-2015 First Flamingo Enterprise B.V.
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
#  TAStop.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 14-Nov-12.
#

from datetime import timedelta
import logging
import re
import xml.sax
import json

from google.appengine.ext import ndb
from google.appengine.api import taskqueue
from ffe.gae import increase_counter
from ffe.ffe_time import cet_from_string, string_from_cet, utc_from_cet
from ffe.markup import XMLImporter
from TABasics import task_name


class StopStatuses:
    announced, extra, canceled, altDestination, finalDestination, planned, revoked = range(7)
    s = ['announced', 'extra', 'canceled', 'altDestination', 'finalDestination', 'planned', 'revoked']


class TAStop(object):

    # Stored attributes:

#-----------------------------------------------------------------------------------|
#   internal variable   |       | external name         | json  | format ( > json)  |
#-----------------------------------------------------------------------------------|
    station_id          = None  # station_id            |   si  |          'nl.asd' |
    mission_id          = None  # mission_id            |   mi  |         'nl.2145' |
    status              = 0     # status                |    s  |                 0 |
    now                 = None  # now                   |  now  | datetime > string |
    arrival             = None  # arrival               |    a  | datetime > string |
    departure           = None  # departure             |    v  | datetime > string |
    delay_arr           = 0.0   # delay_arr             |   da  |               0.0 |
    delay_dep           = 0.0   # delay_dep             |   dv  |               0.0 |
    destination         = None  # destination           |   de  |       'Amsterdam' |
    alteredDestination  = None  # alteredDestination    |   ad  |       'Amsterdam' |
    platform            = None  # platform              |    p  |       '5b' > '5b' |
    platformChange      = False # platformChange        |   pc  |       True > '5b' |
#-----------------------------------------------------------------------------------|



    # ====== Serializing and deserializing ==================================================================

    @property
    def repr(self):
        dictionary = {}
        if self.station_id != None:         dictionary['si'] = self.station_id
        if self.mission_id != None:         dictionary['mi'] = self.mission_id
        if self.status:                     dictionary['s'] = self.status
        if self.arrival != None:            dictionary['a'] = string_from_cet(self.arrival)
        if self.departure != None:          dictionary['v'] = string_from_cet(self.departure)
        if self.now != None:                dictionary['now'] = string_from_cet(self.now)
        if self.delay_arr:                  dictionary['da'] = self.delay_arr
        if self.delay_dep:                  dictionary['dv'] = self.delay_dep
        if self.destination != None:        dictionary['de'] = self.destination
        if self.alteredDestination != None: dictionary['ad'] = self.alteredDestination
        if self.platformChange:
            if self.platform != None:       dictionary['pc'] = self.platform
        else:
            if self.platform != None:       dictionary['p'] = self.platform
        return dictionary

    @classmethod
    def fromRepr(cls, dictionary):
        self = cls()
        self.station_id = dictionary.get('si', None)
        self.mission_id = dictionary.get('mi', None)
        self.status = dictionary.get('s', 0)
        a = dictionary.get('a', None)
        if a: self.arrival = cet_from_string(a)
        v = dictionary.get('v', None)
        if v: self.departure = cet_from_string(v)
        now = dictionary.get('now', None)
        if now: self.now = cet_from_string(now)
        self.delay_arr = dictionary.get('da', 0.0)
        self.delay_dep = dictionary.get('dv', 0.0)
        self.destination = dictionary.get('de', None)
        self.alteredDestination = dictionary.get('ad', None)
        self.platform = dictionary.get('p', None)
        if self.platform != None:
            self.platformChange = False
        else:
            self.platform = dictionary.get('pc', None)
            if self.platform != None:
                self.platformChange = True
        return self

    @property
    def station(self):
        return ndb.Key('TSStation', self.station_id).get()

    @property
    def station_url(self):
        return '/agent/station/%s' % self.station_id

    @property
    def station_code(self):
        comps = self.station_id.split('.')
        return comps[1]

    @property
    def number(self):
        return int(self.mission_id.split('.')[1])

    @property
    def up(self):
        return self.number % 2

    @property
    def est_arrival(self):
        delay = timedelta(minutes=self.delay_arr)
        return self.arrival + delay

    @property
    def arrival_string(self):
        if self.arrival:
            return self.arrival.strftime('%H:%M')
        else:
            return '--'

    @property
    def est_departure(self):
        delay = timedelta(minutes=self.delay_dep)
        return self.departure + delay

    @property
    def departure_string(self):
        if self.departure:
            return self.departure.strftime('%H:%M')
        else:
            return '--'

    @property
    def real_destination(self):
        if self.alteredDestination:
            return self.alteredDestination
        else:
            return self.destination

    @classmethod
    def parse_avt(cls, xml_string, delegate):
        handler = StopsImporter()
        handler.set_up(delegate)
        xml.sax.parseString(xml_string, handler)
        return handler.updated_objects.values()

    @classmethod
    def revoked_stop(cls, mission_id, station_id):
        stop = cls()
        stop.mission_id = mission_id
        stop.station_id = station_id
        stop.status = StopStatuses.revoked
        return stop

    def forward_to_mission(self, issue_time_cet):
        """
        Creates a task in order to forward the stop to its mission
        :param issue_time_cet: the time at which the task will be executed
        :return: a taskqueue.Task that can be issued to the taskqueue
        """
        label = 'fwd_' + self.station_code
        url = '/TAMission/%s' % self.mission_id
        payload = json.dumps(self.repr)
        logging.info('Forward stop to %s at %s CET' % (self.mission_id, issue_time_cet.strftime('%H:%M:%S')))
        issue_time = utc_from_cet(issue_time_cet)
        return taskqueue.Task(name=task_name(issue_time, label),
                              url=url,
                              eta=issue_time,
                              payload=payload,
                              headers={'Content-Type': 'application/json'})


# ====== XML Parser ==================================================================

class StopsImporter(XMLImporter):
    now = None
    delegate = None
    replaced_mission_codes = None
    error = False
    data = None
    train_ref = ''
    stop_status = StopStatuses.announced
    departure = ''
    delay = ''
    destination = ''
    alt_destination = None
    platform = ''
    platform_change = False

    def set_up(self, delegate):
        self.delegate = delegate
        self.replaced_mission_codes = []

    def active_xml_tags(self):
        return ['VertrekkendeTrein']

    def existing_objects_dictionary(self):
        return self.delegate.stops_dictionary

    def key_for_current_object(self):
        if int(self.train_ref) > 0:
            return '%s_%s' % (self.train_ref, self.delegate.code)

    def create_new_object(self, key):
        new_stop = TAStop()
        new_stop.station_id = self.delegate.station_id
        new_stop.mission_id = mission_id_from_code(self.train_ref)
        return new_stop

    def start_xml_element(self, name, attrs):
        if name == 'VertrekkendeTrein':
            self.train_ref = ''
            self.stop_status = StopStatuses.announced
            self.departure = ''
            self.delay = ''
            self.destination = ''
            self.alt_destination = None
            self.platform = ''
            self.platform_change = False

        elif name == 'VertrekSpoor':
            if attrs['wijziging'].lower() == 'true':
                self.platform_change = True

        elif name == 'error':
            self.error = True

    def end_xml_element(self, name):
        if name == 'RitNummer':
            self.train_ref = ''.join(self.data)
            number = int(self.train_ref)
            if number > 1E5:
                original_code = str(number % 100000)
                self.replaced_mission_codes.append(original_code)

        elif name == 'VertrekTijd':
            self.departure = ''.join(self.data)

        elif name == 'VertrekVertraging':
            self.delay = ''.join(self.data)

        elif name == 'EindBestemming':
            self.destination = ''.join(self.data)

        elif name == 'VertrekSpoor':
            self.platform = ''.join(self.data)

        elif name == 'Opmerking':
            remark = ''.join(self.data).strip()
            words = remark.split()
            if words == ['Niet', 'instappen']:
                logging.info('Niet instappen in trein %s', self.train_ref)
            elif words == ['Extra', 'trein']:
                self.stop_status = StopStatuses.extra
            elif words == ['Rijdt', 'vandaag', 'niet']:
                self.stop_status = StopStatuses.canceled
            elif len(words) > 3 and words[0:3] == ['Rijdt', 'verder', 'naar']:
                self.alt_destination = ' '.join(words[3:])
            elif len(words) > 4 and words[0:4] == ['Rijdt', 'niet', 'verder', 'dan']:
                self.alt_destination = ' '.join(words[4:])
            else:
                logging.warning('Unrecognized remark at %s about %s: %s' %
                                (self.delegate.station_id, self.train_ref, remark))

        elif name == 'error':
            raise NSRespondsWithError()

        elif self.error and name == 'message':
            message = ''.join(self.data).strip()
            logging.warning('While requesting departures from %s, server responds: %s' %
                            (self.delegate.station_id, message))

    def update_object(self, existing_object, name):
        if existing_object.status != self.stop_status:
            existing_object.status = self.stop_status
            self.changes = True

        delay = minutes_from_RFC3339_string(self.delay)
        if existing_object.delay_dep != delay:
            existing_object.delay_dep = delay
            self.changes = True

        if existing_object.destination != self.destination:
            existing_object.destination = self.destination
            self.changes = True

        if existing_object.alteredDestination != self.alt_destination:
            existing_object.alteredDestination = self.alt_destination
            self.changes = True

        if existing_object.platform != self.platform:
            existing_object.platform = self.platform
            existing_object.platformChange = self.platform_change
            self.changes = True

        departure = cet_from_string(self.departure)
        if departure != existing_object.departure:
            existing_object.departure = departure
            self.changes = True
        if not hasattr(self, 'last_departure') or departure > self.last_departure:
            self.last_departure = departure

    def save_objects(self):
        for mission_code in self.replaced_mission_codes:
            stop_code = '%s_%s' % (mission_code, self.delegate.code)
            if not self.new_objects.get(stop_code, None):
                stop = self.pop_from_old_objects(stop_code)
                if stop:
                    stop.status = StopStatuses.canceled
                    self.updated_objects[stop_code] = stop
                    self.new_objects[stop_code] = stop

        self.delegate.stops_dictionary = self.new_objects
        self.delegate.nr_of_fetches += 1
        if hasattr(self, 'last_departure'):
            self.delegate.last_departure = self.last_departure
        increase_counter('req_api_success')
        self.delegate.cache_set()


class NSRespondsWithError(Exception):
    pass


# ====== Helpers ==========================================================================

def mission_id_from_code(code):
    if int(code) < 500:
        country = 'eu'
    else:
        country = 'nl'
    return '%s.%s' % (country, code)


def minutes_from_RFC3339_string(string):
    duration = 0.0
    m = re.match(r'PT([0-9]*H)?([0-9]*M)?([0-9]*S)?', string)
    if m:
        if m.group(1):
            duration = 60 * int(m.group(1)[:-1])
        if m.group(2):
            duration += int(m.group(2)[:-1])
        if m.group(3):
            duration += float(m.group(3)[:-1]) / 60
    return duration


def repr_list_from_stops(stops_list):
    if stops_list is None:
        return []
    repr_list = []
    for stop in stops_list:
        repr_list.append(stop.repr)
    return repr_list
