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
#  TSStation.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 30-Apr-14.
#

"""TSStation is a subclass of TSEntry, ..."""

import re, logging
from ffe import config
from google.appengine.ext import ndb
from google.appengine.api import memcache
from ffe.markup import XMLElement, XMLDocument, XMLImporter
from ffe.ffe_time import cet_from_string
from ffe.gae import remote_fetch
from ffe.rest_resources import PublicResource, DataType, NoValidIdentifierError
from TSStationPosition import TSStationPosition
from TSStationAgent import TSStationAgent


class TSStation(PublicResource):
    url_name = 'station'
    agent_url = '/agent/station'
    identifier_regex = re.compile('([a-z]{2})\.([a-z]{1,5})$')
    _positions = None

    # datastore properties:
    names = ndb.StringProperty(repeated=True)
    display_index = ndb.IntegerProperty(indexed=False, default=0)
    label_angle = ndb.IntegerProperty(indexed=False)
    importance = ndb.IntegerProperty()
    wiki_string = ndb.StringProperty(indexed=False)
    opened_string = ndb.StringProperty()

    # ------------ Object lifecycle ------------------------------------------------------------------------------------

    def delete(self):
        for position in self.positions:
            position.delete()
        super(TSStation, self).delete()

    # ------------ Object metadata -------------------------------------------------------------------------------------

    @property
    def country(self):
        return self.id_part(1)

    @property
    def code(self):
        return self.id_part(2)

    # ------------ Finding instances -----------------------------------------------------------------------------------

    @classmethod
    def active_ids(cls):
        memcache_key = '%s_active_ids' % cls.__name__
        ids_list = memcache.get(memcache_key)
        if not ids_list:
            ids_list = []
            for key in cls.query().filter(TSStation.importance <= 3).iter(keys_only=True):
                ids_list.append(key.id())
            memcache.set(memcache_key, ids_list)
        return ids_list

    @classmethod
    def id_for_name(cls, name):
        try:
            return cls.valid_identifier(name)
        except NoValidIdentifierError:
            for key in cls.query().filter(TSStation.names == name).iter(keys_only=True):
                return key.id()

    # ------------ Object properties -----------------------------------------------------------------------------------

    @property
    def name(self):
        if self.names:
            return self.names[0]

    @name.setter
    def name(self, new_name):
        current_display_name = self.display_name
        updated_names = [new_name]
        for existing_name in self.names:
            if existing_name != new_name:
                updated_names.append(existing_name)
        self.names = updated_names
        if current_display_name:
            self.display_name = current_display_name

    @property
    def display_name(self):
        if self.names and len(self.names) > self.display_index:
            return self.names[self.display_index]

    @display_name.setter
    def display_name(self, new_display_name):
        found = False
        for i in range(len(self.names)):
            if self.names[i] == new_display_name:
                self.display_index = i
                found = True
                break
        if not found:
            self.display_index = len(self.names)
            self.names.append(new_display_name)

    def add_alias(self, new_alias):
        if new_alias in self.names:
            return False
        else:
            self.names.append(new_alias)
            return True

    @property
    def wiki_link(self):
        if self.wiki_string:
            components = self.wiki_string.split(':')
            if len(components) == 2:
                string_with_underscores = re.sub(' ', '_', components[1])
                return 'http://%s.wikipedia.org/wiki/%s' % (components[0], string_with_underscores)

    @property
    def agent(self):
        return TSStationAgent.get(self.id_)

    @property
    def positions(self):
        if self._positions is not None:
            return self._positions
        else:
            self._positions = TSStationPosition.query(TSStationPosition.station_key == self.key).fetch(20)
            return self._positions

    def create_position(self, route_code):
        self._positions = None
        return TSStationPosition.new(station_code=self.code, route_code=route_code)

    # ------------ Reading content -------------------------------------------------------------------------------------

    @classmethod
    def xml_handler(cls):
        return StationImporter()

    @classmethod
    def update_stations(cls, file_name=None):
        """
        Updates the current stations with xml-formatted data either from NS-API or a specified file.
        Stations that don't appear in the new data will be deleted.
        :param file_name: Name of the source ('None' redirects to NS-API)
        """
        if file_name:
            fp = open(file_name, 'r')
            xml_string = fp.read()
        else:
            xml_string = remote_fetch(url=config.NSAPI_STATIONS_URL,
                                      headers=config.NSAPI_HEADER,
                                      deadline=config.NSAPI_DEADLINE)
        cls.update_multi(xml_string, DataType.xml)

    def update_with_dictionary(self, dictionary):
        changes = False

        names = dictionary.get('names')
        if names is None:
            name = dictionary.get('name')
            alias = dictionary.get('alias')
            if alias:
                if self.add_alias(name):
                    changes = True
            else:
                if self.name != name:
                    self.name = name
                    changes = True

            if len(self.positions) == 0:
                lat = dictionary.get('lat')
                lon = dictionary.get('lon')
                if lat is not None and lon is not None:
                    self.update_positions([{'km': 0.0, 'route': 'nl.xx00', 'lat': lat, 'lon': lon}])
                    changes = True

        else:
            if names != self.names:
                self.names = names
                changes = True

            display_index = dictionary.get('displayIndex')
            if display_index != self.display_index:
                self.display_index = display_index
                changes = True

            label_angle = dictionary.get('labelAngle')
            if label_angle != self.label_angle:
                self.label_angle = label_angle
                changes = True

            wiki_string = dictionary.get('wikiString')
            if wiki_string != self.wiki_string:
                self.wiki_string = wiki_string
                changes = True

            opened_string = dictionary.get('openedString')
            if opened_string != self.opened_string:
                self.opened_string = opened_string
                changes = True

            if self.update_positions(dictionary.get('positions', [])):
                changes = True

        importance = dictionary.get('importance')
        if importance is not None and importance != self.importance:
            self.importance = importance
            changes = True
        elif importance is self.importance is None:
            self.importance = 3
            changes = True

        return changes

    def update_positions(self, new_positions):
        changes = False
        old_positions = self.positions
        all_positions = []
        updated_positions = []
        for dictionary in new_positions:
            current_position = None
            route_id = dictionary['route']
            for position in old_positions:
                if position.route_id == route_id:
                    current_position = position
                    break
            if current_position is not None:
                old_positions.remove(current_position)
            else:
                route_code = route_id.split('.')[1]
                current_position = self.create_position(route_code)

            current_changes = False
            km = dictionary['km']
            if current_position.km != km:
                current_position.km = km
                current_changes = True
            coordinate = (dictionary['lat'], dictionary['lon'])
            if current_position.coordinate != coordinate:
                current_position.coordinate = coordinate
                current_changes = True
            all_positions.append(current_position)
            if current_changes:
                changes = True
                updated_positions.append(current_position)
        self._positions = None
        for position in old_positions:
            position.delete()
        if updated_positions:
            ndb.put_multi(updated_positions)
        return changes

    # ------------ Writing content -------------------------------------------------------------------------------------

    @property
    def xml(self):
        return XMLElement('station', {'id': self.id_, 'name': self.name, 'importance': self.importance})

    @property
    def xml_document(self):
        document = XMLDocument('routeItems')
        document.root.add(self.xml)
        return document

    def dictionary_from_object(self):
        dictionary = {'id': self.id_}

        if self.names is not None:
            dictionary['names'] = self.names

        if self.display_index is not None:
            dictionary['displayIndex'] = self.display_index

        if self.label_angle is not None:
            dictionary['labelAngle'] = self.label_angle

        if self.importance is not None:
            dictionary['importance'] = self.importance

        if self.wiki_string is not None:
            dictionary['wikiString'] = self.wiki_string

        if self.opened_string is not None:
            dictionary['openedString'] = self.opened_string

        positions = []
        for position in self.positions:
            positions.append(position.dictionary_from_object(perspective='station'))
        if positions:
            dictionary['positions'] = positions

        return dictionary


# ====== XML Parser ====================================================================================================

class StationImporter(XMLImporter):

    now = None
    from_rail_atlas = False
    name = None
    code = None
    country = None
    alias = False
    importance = None
    lat = None
    lon = None

    def active_xml_tags(self):
        return ['station']

    def existing_objects_dictionary(self):
        return TSStation.objects_dictionary()

    def key_for_current_object(self):
        if self.code and self.country == 'nl':
            return '%s.%s' % (self.country, self.code)

    def create_new_object(self, key):
        return TSStation.new(key)

    def start_xml_element(self, name, attrs):
        if name == 'routeItems':
            self.from_rail_atlas = True

        elif name == 'TAStation':
            self.from_rail_atlas = True

        elif name == 'station':
            if self.from_rail_atlas:
                identifier = attrs.get('id')
                comps = identifier.split('.')
                self.country = comps[0]
                self.code = comps[1]
                self.name = attrs.get('name')
                self.importance = int(attrs.get('importance'))

        elif name == 'unit_test':
            now_string = attrs.get('timestamp')
            self.now = cet_from_string(now_string)

    def end_xml_element(self, name):
        if not self.from_rail_atlas:
            if name == 'name':
                self.name = ''.join(self.data)

            elif name == 'code':
                self.code = ''.join(self.data).lower()

            elif name == 'country':
                self.country = ''.join(self.data).lower()

            elif name == 'alias':
                string = ''.join(self.data).lower()
                if string == 'true':
                    self.alias = True
                else:
                    self.alias = False

            elif name == 'lat':
                self.lat = float(''.join(self.data))

            elif name == 'long':
                self.lon = float(''.join(self.data))

    def update_object(self, existing_object, name):
        dictionary = {'alias': self.alias,
                      'name': self.name,
                      'importance': self.importance,
                      'lat': self.lat,
                      'lon': self.lon}
        if existing_object.update_with_dictionary(dictionary):
            self.changes = True

    def save_objects(self):
        assert len(self.old_objects) < 10
        if self.updated_objects:
            ndb.put_multi(self.updated_objects.values())
        for station in self.old_objects.itervalues():
            station.delete()

