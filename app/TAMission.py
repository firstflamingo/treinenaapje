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
#  TAMission.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 21-Feb-13.
#

import json, logging, random

from google.appengine.ext   import db
from datetime               import datetime, time, timedelta

from ffe                    import config
from ffe.gae                import increase_counter, issue_tasks
from ffe.markup             import XMLElement
from ffe.ffe_time           import now_cet, mark_cet
from TABasics               import TAModel
from TAStop                 import TAStop, StopStatuses, repr_list_from_stops

# ========== Mission Model ==========================================================================


class MissionStatuses:
    inactive, announced, running, arrived, completed, canceled, ambivalent = range(7)
    s = ['inactive', 'announced', 'running', 'arrived', 'completed', 'canceled', 'ambivalent']


class ODIDsProperty(db.TextProperty):

    def validate(self, value):
        return value

    def get_value_for_datastore(self, model_instance):
        dictionary = super(ODIDsProperty, self).get_value_for_datastore(model_instance)
        if dictionary and len(dictionary) > 3:
            dictionary = optimize_od_ids(dictionary)
        serialized = json.dumps(dictionary)
        return db.Text(serialized)

    def make_value_from_datastore(self, value):
        dictionary = json.loads(str(value))
        return super(ODIDsProperty, self).make_value_from_datastore(dictionary)


class StopsListProperty(db.TextProperty):
    def validate(self, value):
        return value

    def get_value_for_datastore(self, model_instance):
        stopsList = super(StopsListProperty, self).get_value_for_datastore(model_instance)
        serialized = json.dumps(repr_list_from_stops(stopsList))
        return db.Text(serialized)

    def make_value_from_datastore(self, value):
        reprList = json.loads(str(value))
        stopsList = []
        for repr in reprList:
            stopsList.append(TAStop.fromRepr(repr))
        return super(StopsListProperty, self).make_value_from_datastore(stopsList)


class TAMission(TAModel):

    # Stored attributes:
    series_id           = db.StringProperty()
    _odIDsDictionary    = ODIDsProperty()
    nominalDate         = db.DateProperty(indexed=False)
    offset              = db.TimeProperty(indexed=False)
    _stops              = StopsListProperty()

    # Transient attributes
    _number             = None
    delay               = 0.0
    delay_update_limit  = None
    needs_datastore_put = False
    issue_time          = None
    tasks               = None

    # ------ Object lifecycle ---------------------------------------------

    def awake_from_create(self):
        series = self.series
        if self.supplementary:
            original = self.original_mission
            if original:
                self.offset = original.offset
                if series:
                    series.add_mission(self)

    # ------ Mission identity ---------------------------------------------

    @property
    def number(self):
        if self._number is None:
            self._number = int(self.code)
        return self._number

    @property
    def base_number(self):
        return self.number % 100000

    @property
    def ordinal(self):
        if self.country == 'eu':
            return self.number % 10
        else:
            return self.number % 100

    @property
    def up(self):
        return self.number % 2

    # ------ Defining the mission -----------------------------------------

    @property
    def offset_time(self):
        if self.offset:
            return self.offset
        else:
            return time(0)

    @offset_time.setter
    def offset_time(self, value):
        if value != self.offset:
            self.offset = value
            if not self.supplementary:
                self.needs_datastore_put = True

    @property
    def offset_cet(self):
        return mark_cet(datetime.combine(self.nominalDate, self.offset_time))

    @offset_cet.setter
    def offset_cet(self, value):
        rounded_time = round_mission_offset(value)
        self.nominalDate = rounded_time.date()
        self.offset = rounded_time.time()
        if not self.supplementary:
            self.needs_datastore_put = True

    @property
    def offset_string(self):
        if self.offset is None:
            return '-'
        else:
            return self.offset.strftime('%H:%M')

    @property
    def date_string(self):
        if self.nominalDate is None:
            return '-'
        else:
            return self.nominalDate.strftime('%d-%m-%Y')

    @offset_string.setter
    def offset_string(self, value):
        if value == '-':
            new_offset = None
        else:
            dt = mark_cet(datetime.strptime(value, '%H:%M'))
            new_offset = dt.time()
        self.offset_time = new_offset

    @property
    def weekday(self):
        if self.nominalDate in config.OFFICIAL_HOLIDAYS:
            return 6
        return self.nominalDate.weekday()

    @property
    def supplementary(self):
        return self.number // 100000

    @property
    def original_mission(self):
        if self.supplementary:
            return TAMission.get('%s.%d' % (self.country, self.base_number))
        else:
            return self

    # ------ Connecting to series -----------------------------------------

    @property
    def series_number(self):
        if self.country == 'eu':
            return self.base_number // 10
        else:
            return self.base_number // 100

    @property
    def series(self):
        if self.series_id == 'orphan':
            return None

        if not self.series_id:
            nr = int(self.number % 1E5)
            if self.country == 'eu':
                self.series_id = config.INTERNATIONAL_SERIES.get(nr//10, 'eu.000')
            else:
                self.series_id = '%s.%03d' % (self.country, nr // 100)

        series = TAModel.get(self.series_id, class_name='TASeries')
        if not series:
            self.series_id = 'orphan'
        return series

    # ------ Managing origin/destination IDs ----------------------------

    @property
    def odIDs_dictionary(self):
        if not self._odIDsDictionary:
            self._odIDsDictionary = {'d': [None, None]}
        return self._odIDsDictionary

    @odIDs_dictionary.setter
    def odIDs_dictionary(self, new_dict):
        if new_dict != self._odIDsDictionary:
            self._odIDsDictionary = new_dict
            self.needs_datastore_put = True

    def get_odIDs_for_weekday(self, weekday):
        key = str(weekday)
        return self.odIDs_dictionary.get(key, self.odIDs_dictionary['d'])

    def set_odIDs_for_weekday(self, weekday, new_ids):
        if self.get_odIDs_for_weekday(weekday) != new_ids:
            self.odIDs_dictionary[str(weekday)] = new_ids
            self.needs_datastore_put = True

    @property
    def odIDs(self):
        return self.get_odIDs_for_weekday(self.weekday)

    @odIDs.setter
    def odIDs(self, newIDs):
        self.set_odIDs_for_weekday(self.weekday, newIDs)

    def get_odIDs_string(self, weekday):
        ids = self.get_odIDs_for_weekday(weekday)
        if ids[0] is None:
            return '-'
        else:
            return '%s-%s' % (ids[0], ids[1])

    def set_odIDs_string(self, weekday, string):
        if string == '-':
            ids = [None, None]
        else:
            comps = string.split('-')
            ids = [comps[0], comps[1]]
        self.set_odIDs_for_weekday(weekday, ids)

    @property
    def origin_id(self):
        return self.odIDs[0]

    @origin_id.setter
    def origin_id(self, new_id):
        self.odIDs = [new_id, self.destination_id]

    @property
    def destination_id(self):
        return self.odIDs[1]

    @destination_id.setter
    def destination_id(self, new_id):
        self.odIDs = [self.origin_id, new_id]

    def optimize_odIDs_dictionary(self):
        self.odIDs_dictionary = optimize_od_ids(self.odIDs_dictionary)

    # ------ Providing output -----------------------------------------

    @property
    def stops_repr(self):
        return repr_list_from_stops(self.stops)

    @property
    def repr(self):
        return {'id': self.id, 'stops': self.stops_repr}

    @property
    def xml(self):
        array = []
        for key, value in self.odIDs_dictionary.iteritems():
            array.append(XMLElement('od', {'day': key, 'from': value[0], 'to': value[1]}, []))
        return XMLElement('mission', {'id': self.id, 'offset': self.offset_time.strftime('%H:%M')}, array)

    # ------ Connecting to ScheduledPoint ----------------------------------------

    def create_stop_from_point(self, point):
        stop = TAStop()
        stop.station_id = point.station_id
        stop.mission_id = self.id
        stop.status = StopStatuses.planned
        stop.arrival = self.offset_cet + self.arrival_delta_with_point(point)
        stop.departure = self.offset_cet + self.departure_delta_with_point(point)
        stop.delay_dep = 0.0
        stop.platform = point.platform_string(self.up)
        return stop

    def arrival_delta_with_point(self, point):
        if self.up:
            return timedelta(minutes=point.upArrival)
        else:
            return timedelta(minutes=point.downArrival)

    def departure_delta_with_point(self, point):
        if self.up:
            return timedelta(minutes=point.upDeparture)
        else:
            return timedelta(minutes=point.downDeparture)

    def pattern_minutes_at_stop(self, stop):
        dt = stop.departure - self.offset_cet
        return dt.seconds // 60

    # ------ Managing stops --------------------------------------------

    @property
    def stops(self):
        if self._stops is None:
            self._stops = []
        return self._stops

    @stops.setter
    def stops(self, stops):
        self._stops = stops

    @property
    def first_stop(self):
        if self._stops:
            return self.stops[0]
        else:
            return None

    @property
    def last_stop(self):
        if self._stops:
            return self.stops[-1]
        else:
            return None

    @property
    def est_arrival_cet(self):
        est_arrival = self.last_stop.arrival
        if not est_arrival:
            est_arrival = self.last_stop.departure
        return est_arrival + timedelta(minutes=self.last_stop.delay_dep)

    @property
    def destination(self):
        if not self.last_stop:
            return 'no stops'
        last_station = self.last_stop.station.name
        announced_destination = self.last_stop.destination
        if last_station == announced_destination:
            return last_station
        else:
            return '%s (%s)' % (last_station, announced_destination)

    def awake_stops(self):
        series = self.series
        from_id, to_id = self.odIDs
        all_stops = []
        for point in series.points_in_range(from_id, to_id):
            new_stop = self.create_stop_from_point(point)
            all_stops.append(new_stop)
        if all_stops:
            all_stops[-1].status = StopStatuses.finalDestination
            last_station = all_stops[-1].station
            for stop in all_stops:
                stop.destination = last_station.name
        self.stops = all_stops

    def update_stop(self, updated):
        now = updated.now
        if now is None:
            now = now_cet()
        status, delay = self.status_at_time(now)
        if status == MissionStatuses.arrived:
            logging.info('Update was ignored because mission has already arrived')
            return

        changes = False
        small_changes = False
        self.issue_time = now
        index = self.index_for_stop(updated)
        if index is not None:
            existing = self.stops[index]
            self.tasks = []

            if existing.status != updated.status:
                logging.info('Change status at %s from %s to %s.' % (existing.station_id,
                                                                     StopStatuses.s[existing.status],
                                                                     StopStatuses.s[updated.status]))
                if updated.status == StopStatuses.revoked:
                    self.remove_stop(index)
                    existing = None
                    changes = True
                else:
                    if existing.status == StopStatuses.planned and updated.status == StopStatuses.announced:
                        small_changes = True
                    elif existing.status == StopStatuses.altDestination and updated.status == StopStatuses.announced:
                        self.reset_destination()
                        changes = True
                    else:
                        if updated.status == StopStatuses.canceled:
                            self.check_for_canceled(index - 1)
                            self.check_for_canceled(index + 1)
                        elif existing.status == StopStatuses.canceled:
                            self.check_for_uncanceled(index - 1)
                            self.check_for_uncanceled(index + 1)
                        changes = True
                    existing.status = updated.status

            if existing is not None:
                if existing.delay_dep != updated.delay_dep:
                    logging.info('Change delay at %s from %.1f to %.1f.' %
                                 (existing.station_id, existing.delay_dep, updated.delay_dep))
                    next_index = self.next_stop_index(now)
                    if index == next_index:
                        increasing = bool(existing.delay_dep < updated.delay_dep)
                        self.update_delay(index, updated.delay_dep, increasing)
                        self.schedule_more_updates(updated, now)
                    else:
                        if next_index is not None and existing.delay_dep == 0:
                            next_stop = self.stops[next_index]
                            self.issue_time += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
                            self.tasks.append(self.instruction_task(next_stop.station_url, 'prio', self.issue_time))
                        existing.delay_dep = updated.delay_dep
                    changes = True

                if existing.platform != updated.platform and updated.platform is not None:
                    logging.info('Change platform at %s from %s to %s.' %
                                 (existing.station_id, existing.platform, updated.platform))
                    existing.platform = updated.platform
                    changes = True
                    if existing.platformChange != updated.platformChange:
                        existing.platformChange = updated.platformChange

                if existing.destination != updated.destination and updated.destination is not None:
                    logging.info('Change destination at %s from %s to %s.' %
                                 (existing.station_id, existing.destination, updated.destination))
                    existing.destination = updated.destination
                    changes = True
                    self.update_destination(updated.destination)

                if existing.alteredDestination != updated.alteredDestination:
                    logging.info('Change altered destination at %s from %s to %s.' %
                                 (existing.station_id, existing.alteredDestination, updated.alteredDestination))
                    if updated.alteredDestination is None:
                        self.reset_destination()
                    else:
                        self.alter_destination(updated.alteredDestination)
                    existing.alteredDestination = updated.alteredDestination
                    changes = True

                if existing.departure != updated.departure and updated.departure is not None:
                    logging.info('Change departure at %s from %s to %s.' %
                                 (existing.station_id, existing.departure.strftime('%H:%M'), updated.departure.strftime('%H:%M')))
                    logging.info('%s ==> %s' % (existing.departure, updated.departure))
                    delta = updated.departure - existing.departure
                    existing.arrival += delta
                    existing.departure = updated.departure
                    changes = True

            issue_tasks(self.tasks)
            self.tasks = None

        else:
            if updated.status == StopStatuses.announced or updated.status == StopStatuses.extra:
                self.anterior_stops(updated)
                changes = True
        if changes:
            increase_counter('mission_changes')
            self.put()
        else:
            if small_changes:
                increase_counter('mission_small_changes')
                self.cache_set()
            else:
                increase_counter('mission_no_changes')

    def remove_stop(self, index):
        if index == 0:
            if len(self.stops) == 1 or len(self.stops) == 2 and self.stops[1].status == StopStatuses.finalDestination:
                self.stops = []
                self.odIDs = [None, None]
                logging.info('Mission is not running today')
            else:
                self.origin_id = self.stops[1].station_id
                logging.info('Mission origin changed to %s' % self.origin_id)
                del self.stops[0]
        else:
            station_name = self.stops[index].station.name
            is_destination = False
            i = index - 1
            while i >= 0:
                stop = self.stops[i]
                if stop.status == StopStatuses.announced:
                    if stop.destination == station_name:
                        is_destination = True
                    break
                i -= 1
            if is_destination:
                new_stops = self.stops[0:index+1]
                self.stops = new_stops
                self.stops[index].status = StopStatuses.finalDestination
                self.destination_id = self.stops[index].station_id
                logging.info('Mission destination changed to %s' % self.destination_id)
            else:
                logging.info('Mission does not stop at %s' % self.stops[index].station_id)
                del self.stops[index]

    def check_for_canceled(self, index):
        """
        Checks whether stop at index must receive a prio-request and adds it to the task-list if needed
        """
        if 0 <= index < len(self.stops):
            stop = self.stops[index]
            if stop.status == StopStatuses.planned or stop.status == StopStatuses.announced:
                self.issue_time += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
                self.tasks.append(self.instruction_task(stop.station_url, 'prio', self.issue_time))

    def check_for_uncanceled(self, index):
        """
        Checks whether stop at index must receive a prio-request and adds it to the task-list if needed
        """
        if 0 <= index < len(self.stops):
            stop = self.stops[index]
            if stop.status == StopStatuses.canceled:
                self.issue_time += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
                self.tasks.append(self.instruction_task(stop.station_url, 'prio', self.issue_time))

    def update_delay(self, index, delay, increasing):
        """
        Updates the delay for index and higher
        """
        stop = self.stops[index]
        stop.delay_dep = delay
        self.delay = delay

        index += 1
        while index < len(self.stops):
            previous_stop = stop
            stop = self.stops[index]
            riding_time = stop.arrival - previous_stop.departure
            margin = (riding_time.seconds * config.RIDING_TIME_MARGIN) / 60
            if stop.status == StopStatuses.planned or stop.status == StopStatuses.announced:
                stop_time = stop.departure - stop.arrival
                if stop_time.seconds > config.MINIMUM_STOP_TIME:
                    margin += (stop_time.seconds - config.MINIMUM_STOP_TIME) / 60
            delay -= margin
            if delay < 0:
                if increasing:
                    break
                delay = 0
            if increasing:
                if delay > stop.delay_dep:
                    stop.delay_dep = delay
            else:
                if delay < stop.delay_dep:
                    stop.delay_dep = delay
            index += 1

    def index_for_stop(self, searched_stop):
        for index, stop in enumerate(self.stops):
            if searched_stop.station_id == stop.station_id:
                return index
        return None

    def next_stop_index(self, now=None):
        if not now:
            now = now_cet()
        for index in range(len(self.stops)):
            if now < self.stops[index].est_departure:
                return index
        return None

    def anterior_stops(self, new_stop):
        series = self.series
        found_index = None

        if series:
            found_index = series.index_for_station(new_stop.station_id)
        if not self.nominalDate:
            self.nominalDate = new_stop.departure.date()

        if found_index is not None:
            logging.info('Insert new stops in mission %s triggered by %s...' % (self.id, new_stop.station_id))
            found_point = series.point_at_index(found_index)
            if self.offset is None:
                self.offset_cet = new_stop.departure - self.departure_delta_with_point(found_point)
                series.add_mission(self)
            arrival = self.offset_cet + self.arrival_delta_with_point(found_point)
            if arrival <= new_stop.departure:
                new_stop.arrival = arrival
            else:
                new_stop.arrival = new_stop.departure

            insert_destination = bool(self.destination_id is None)
            destination = new_stop.destination
            if insert_destination:
                index = series.index_for_station(destination)
                if index is None:
                    if self.up:
                        index = len(series.points) - 1
                    else:
                        index = 0
                identifier = series.point_at_index(index).station_id
                logging.info('New destination = %s' % identifier)
                self.odIDs = (identifier, identifier)
            else:
                logging.info('Existing destination = %s' % self.destination_id)

            current_stops = self.stops
            existing_station_id = None
            if current_stops:
                existing_station_id = current_stops[0].station_id
            else:
                current_stops = []

            # search for more scheduledPoints that are not yet in the mission:
            index = found_index
            limit = series.index_for_station(self.origin_id)
            while True:
                logging.info('New stop = %s' % new_stop.station_id)
                self.stops.append(new_stop)
                if limit is None:
                    break
                if self.up:
                    index += 1
                    if index > limit:
                        break
                else:
                    index -= 1
                    if index < limit:
                        break
                point = series.point_at_index(index)
                if point.station_id == existing_station_id:
                    break
                new_stop = self.create_stop_from_point(point)
                new_stop.destination = destination

            # replace stops list:
            self.sort_stops()
            if self.first_stop.station_id == found_point.station_id:
                logging.info('Origin = %s', found_point.station_id)
                self.origin_id = found_point.station_id

            if insert_destination:
                self.last_stop.status = StopStatuses.finalDestination

        else:
            logging.warning('Insert unrecognized stop %s in mission %s.' % (new_stop.station_id, self.id))

            # stop sent by TAStation does not have arrival information
            new_stop.arrival = new_stop.departure
            new_stop.delay_arr = new_stop.delay_dep
            self.stops.append(new_stop)
            self.sort_stops()

    def sort_stops(self):
        t = []
        for stop in self.stops:
            t.append((stop.departure, stop))
        t.sort()
        result = []
        for theTime, stop in t:
            result.append(stop)
        self.stops = result

    def update_destination(self, destination):
        series = self.series
        if series:
            logging.info('update destination to %s for mission %s' % (destination, self.code))
            new_index = series.index_for_station(destination)
            if new_index is None:
                if self.up:
                    new_index = len(series.points) - 1
                else:
                    new_index = 0
            to_index = series.index_for_station(self.destination_id)
            insert_range = []
            if self.up:
                if new_index > to_index:
                    insert_range = range(to_index + 1, new_index + 1)
                    self.destination_id = series.point_at_index(new_index).station_id
            else:
                if new_index < to_index:
                    insert_range = range(new_index, to_index)
                    insert_range.reverse()
                    self.destination_id = series.point_at_index(new_index).station_id
            if insert_range:
                if self.last_stop.status == StopStatuses.finalDestination:
                    self.last_stop.status = StopStatuses.planned
                all_stops = self.stops
                for index in insert_range:
                    the_point = series.point_at_index(index)
                    newStop = self.create_stop_from_point(the_point)
                    newStop.destination = destination
                    all_stops.append(newStop)
                    logging.info('insert stop %s' % newStop.station_id)
                self.stops = all_stops
                self.last_stop.status = StopStatuses.finalDestination

    def alter_destination(self, destination):
        """
        Determines which stop is the new altDestination
        Adjusts status and altDestination in the stops list to the new altDestination
        Adds a task to the task list, to check what's going on at altDestination
        """
        series = self.series
        if not series:
            logging.warning('Cannot alter destination to %s for orphan mission %s.' % (destination, self.id))
            return

        destination_point = series.point_for_station(destination)
        if not destination_point:
            logging.warning('Cannot alter destination to %s for mission %s. (no id found)' % (destination, self.id))
            return

        destination_id = destination_point.station_id
        passed = False
        for stop in self.stops:
            if passed:
                stop.status = StopStatuses.canceled
            else:
                if stop.station_id == destination_id:
                    passed = True
                    stop.status = StopStatuses.altDestination
                else:
                    stop.alteredDestination = destination

        if passed:
            logging.info('Mission %s altered destination to %s.' % (self.id, destination))
        else:
            logging.warning('Mission %s could not find altered destination %s.' % (self.id, destination))
        url = '/agent/station/%s' % destination_id
        self.issue_time += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
        self.tasks.append(self.instruction_task(url, 'prio', self.issue_time))

    def reset_destination(self):
        for stop in self.stops:
            stop.alteredDestination = None
            if stop.status == StopStatuses.canceled:
                stop.status = StopStatuses.planned
        self.last_stop.status = StopStatuses.finalDestination
        logging.info('Reset normal destination for mission %s.' % self.id)

    # ------ managing status ----------------------------------------

    def status_at_time(self, now=None):
        if len(self.stops) == 0:
            return MissionStatuses.inactive, 0.0

        if now is None:
            now = now_cet()
        if now > self.est_arrival_cet:
            return MissionStatuses.arrived, self.last_stop.delay_dep

        index = self.next_stop_index(now)
        stop = self.stops[index]
        if stop.status == StopStatuses.canceled:
            return MissionStatuses.canceled, stop.delay_dep
        if index == 0:
            if stop.status == StopStatuses.announced:
                return MissionStatuses.announced, stop.delay_dep
            else:
                return MissionStatuses.inactive, 0.0

        return MissionStatuses.running, stop.delay_dep

    def activate_mission(self, now=None):
        if now is None:
            now = now_cet()
        if self.offset is None:
            self.offset = time(0)
            self.needs_datastore_put = True
        self.nominalDate = now.date()
        if self.destination_id is None:
            self.stops = []
        else:
            self.awake_stops()
            self.check_mission_announcements(now)

    def check_mission_announcements(self, issue_time):
        tasks = []
        reference_time = issue_time + timedelta(minutes=config.PERIOD_FOR_ANNOUNCEMENT_CHECKS)
        passed = False
        for stop in self.stops:
            if stop.status == StopStatuses.planned and issue_time < stop.departure:
                if not passed and stop.departure < reference_time:
                    passed = True
                    issue_time += timedelta(seconds=config.INTERVAL_BETWEEN_UPDATE_MSG)
                    tasks.append(self.instruction_task(stop.station_url, 'check', issue_time, expected=stop.departure))
                else:
                    next_check = stop.departure - timedelta(minutes=config.TIME_PRIOR_TO_ANNOUNCEMENT)
                    tasks.append(self.instruction_task(self.url, 'check', next_check, random_s=True))
                    break
        issue_tasks(tasks)

    def schedule_more_updates(self, stop, now):
        """
        Provides a list with tasks to further check how the delay of the mission develops
        """
        if not hasattr(self, 'delay_update_limit') or self.delay_update_limit is None or self.delay_update_limit < now:
            self.delay_update_limit = now

        new_limit = stop.est_departure + timedelta(minutes=(stop.delay_dep / config.RIDING_TIME_MARGIN))
        arrival = self.last_stop.arrival + timedelta(minutes=self.last_stop.delay_dep)
        if new_limit > arrival:
            new_limit = arrival
        new_limit -= timedelta(seconds=config.DELAY_UPDATE_INTERVAL)

        while self.delay_update_limit < new_limit:
            self.delay_update_limit += timedelta(seconds=config.DELAY_UPDATE_INTERVAL)
            next_stop = self.stops[self.next_stop_index(self.delay_update_limit)]
            task = self.instruction_task(next_stop.station_url, 'prio', self.delay_update_limit, random_s=True)
            self.tasks.append(task)

    # Archiving
    def remove(self):
        memcache.delete(self.id, namespace='TAMission')


# ====== Helper functions ======================================================================

def stochastic_round(number):
    whole = int(number)
    fraction = number % 1
    if fraction < random.random():
        return whole
    else:
        return whole + 1


def round_mission_offset(the_time, amount=2):
    if the_time.minute in range(60 - amount, 60):
        the_time = the_time.replace(minute=0)
        the_time += timedelta(hours=1)
    if the_time.minute in range(1, 1 + amount):
        the_time = the_time.replace(minute=0)
    if the_time.minute in range(30 - amount, 31 + amount):
        the_time = the_time.replace(minute=30)
    return the_time


def optimize_od_ids(dictionary):
    histogram = {}
    array = []
    maximum = 0
    max_value = None
    for index in range(7):
        value = dictionary.get(str(index), dictionary['d'])
        array.append(value)
        key = json.dumps(value)
        count = histogram.get(key, 0) + 1
        histogram[key] = count
        if count > maximum:
            maximum = count
            max_value = value
    if maximum > 1:
        optimized_dict = {'d': max_value}
        for index in range(7):
            if array[index] != max_value:
                optimized_dict[str(index)] = array[index]
        return optimized_dict
