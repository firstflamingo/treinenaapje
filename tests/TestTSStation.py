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
#  TestTSStation.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 30-Apr-14.
#

import logging, unittest, json
import webapp2, webtest
from google.appengine.api   import memcache
from google.appengine.ext import ndb, testbed
from atlas_api import ATLAS_URL_SCHEMA
from TSStation import TSStation
from TSStationPosition import TSStationPosition
from TSAdmin import TSAdmin
from ffe.ffe_time import rfc1123_from_utc
from ffe.ffe_utils import auth_header

class TestTSStation(unittest.TestCase):

    def setUp(self):
        app = webapp2.WSGIApplication(ATLAS_URL_SCHEMA)
        self.testapp = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

        logger = logging.getLogger()
        logger.level = logging.DEBUG

    def tearDown(self):
        self.testbed.deactivate()

    def test_naming(self):

        # Setting name, display_name and alias:
        station = TSStation.new('nl.test')
        station.name = 'name 1'
        self.assertEqual(station.name, 'name 1')
        self.assertEqual(station.display_name, 'name 1')
        self.assertEqual(len(station.names), 1)
        station.display_name = 'name 2'
        self.assertEqual(station.name, 'name 1')
        self.assertEqual(station.display_name, 'name 2')
        self.assertEqual(len(station.names), 2)
        station.name = 'name 2'
        self.assertEqual(station.name, 'name 2')
        self.assertEqual(station.display_name, 'name 2')
        self.assertEqual(len(station.names), 2)
        station.name = 'name 3'
        self.assertEqual(station.name, 'name 3')
        self.assertEqual(station.display_name, 'name 2')
        self.assertEqual(len(station.names), 3)
        result = station.add_alias('name 1')
        self.assertFalse(result)
        self.assertEqual(len(station.names), 3)
        result = station.add_alias('name 4')
        self.assertTrue(result)
        self.assertEqual(station.name, 'name 3')
        self.assertEqual(station.display_name, 'name 2')
        self.assertEqual(len(station.names), 4)

        # Finding station by name
        station.put()
        self.assertEqual(TSStation.id_for_name('name 1'), 'nl.test')
        self.assertEqual(TSStation.id_for_name('nl.test'), 'nl.test')
        self.assertIsNone(TSStation.id_for_name('name 5'))

    def test_xml_parsing(self):

        # Step 1: Import Rijswijk and Delft from Rail Atlas
        TSStation.update_stations('TestTSStation.data/routeItems1.xml')
        self.assertEqual(TSStation.all_ids(), ['nl.dt', 'nl.rsw'])

        rijswijk = TSStation.get('nl.rsw')
        self.assertEqual(rijswijk.name, 'Rijswijk')
        self.assertEqual(rijswijk.importance, 4)
        delft = TSStation.get('nl.dt')
        self.assertEqual(delft.name, 'Delft')
        self.assertEqual(delft.importance, 2)

        expected = '<?xml version="1.0" encoding="UTF-8"?>' \
                   '<TSStation><station importance="2" id="nl.dt" name="Delft"/>' \
                   '<station importance="4" id="nl.rsw" name="Rijswijk"/></TSStation>'
        self.assertEqual(TSStation.xml_catalog().write(), expected)

        # Step 2: Import Delft Zuid and Schiedam from NS-API
        TSStation.update_stations('TestTSStation.data/NS-API-1.xml')
        self.assertEqual(TSStation.all_ids(), ['nl.dt', 'nl.dtz', 'nl.rsw', 'nl.sdm'])
        self.assertEqual(TSStation.active_ids(), ['nl.dt', 'nl.dtz', 'nl.sdm'])
        cached_ids = memcache.get('TSStation_active_ids')
        self.assertEqual(cached_ids, ['nl.dt', 'nl.dtz', 'nl.sdm'])

        rijswijk = TSStation.get('nl.rsw')
        self.assertEqual(rijswijk.name, 'Rijswijk')
        self.assertEqual(rijswijk.importance, 4)
        delft = TSStation.get('nl.dt')
        self.assertEqual(delft.name, 'Delft')
        self.assertEqual(delft.importance, 2)
        delft_zuid = TSStation.get('nl.dtz')
        self.assertEqual(delft_zuid.name, 'Delft Zuid')
        self.assertEqual(delft_zuid.importance, 3)
        self.assertEqual(len(delft_zuid.names), 2)
        self.assertEqual(len(delft_zuid.positions), 1)
        schiedam = TSStation.get('nl.sdm')
        self.assertEqual(schiedam.name, 'Schiedam')
        self.assertEqual(schiedam.importance, 3)
        self.assertEqual(len(schiedam.names), 2)
        self.assertEqual(len(schiedam.positions), 1)

        # Step 3: Overwrite data with Rail Atlas --> Schiedam is removed
        TSStation.update_stations('TestTSStation.data/routeItems2.xml')

        rijswijk = TSStation.get('nl.rsw')
        self.assertEqual(rijswijk.name, 'Rijswijk')
        self.assertEqual(rijswijk.importance, 4)
        delft = TSStation.get('nl.dt')
        self.assertEqual(delft.name, 'Delft')
        self.assertEqual(delft.importance, 1)
        delft_zuid = TSStation.get('nl.dtz')
        self.assertEqual(delft_zuid.name, 'Delft Zuid')
        self.assertEqual(delft_zuid.importance, 1)
        schiedam = TSStation.get('nl.sdm')
        self.assertIsNone(schiedam)

        # Step 4: Overwrite data with NS-API --> Rijswijk is removed
        TSStation.update_stations('TestTSStation.data/NS-API-2.xml')

        rijswijk = TSStation.get('nl.rsw')
        self.assertIsNone(rijswijk)
        delft = TSStation.get('nl.dt')
        self.assertEqual(delft.name, 'Delft Centrum')
        self.assertEqual(delft.importance, 1)
        delft_zuid = TSStation.get('nl.dtz')
        self.assertEqual(delft_zuid.name, 'Delft Tanthof')
        self.assertEqual(delft_zuid.importance, 1)

    def test_crud_capabilities(self):
        """
        Test the CRUD cycle (Create, Read, Update, Delete)
        """

        station = TSStation.new('nl.test')
        station.names.append('Appingedam')
        self.assertEqual(station.id_, 'nl.test')
        self.assertEqual(station.code, 'test')
        self.assertEqual(station.country, 'nl')
        self.assertEqual(station.url, '/station/nl.test')
        station.put()

        # Read the entry
        station = TSStation.get('nl.test')
        self.assertEqual(station.id_, 'nl.test')
        self.assertEqual(station.name, 'Appingedam')

        # Update the entry
        station.names.append('Appingedam Centrum')
        station.put()
        station = TSStation.get('nl.test')
        self.assertEqual(station.names[1], 'Appingedam Centrum')

        # Update with dictionary
        dictionary = {
            'id': 'nl.test',
            'names': ['Amsterdam Muiderpoort', 'Muiderpoort'],
            'displayIndex': 1,
            'labelAngle': 180,
            'importance': 3,
            'wikiString': 'nl:Station Amsterdam Muiderpoort',
            'openedString': '1896-05-18',
            'positions': [
                {'km': 14.350, 'route': 'nl.os01', 'lat': 52.3605540, 'lon': 4.9311113},
                {'km': 0.398, 'route': 'nl.ssh01', 'lat': 52.3605540, 'lon': 4.9311113}
            ]}
        station.update_with_dictionary(dictionary)
        station = TSStation.get('nl.test')
        self.assertEqual(station.dictionary_from_object(), dictionary)
        self.assertEqual(station.name, 'Amsterdam Muiderpoort')
        self.assertEqual(station.display_name, 'Muiderpoort')
        self.assertEqual(station.wiki_link, 'http://nl.wikipedia.org/wiki/Station_Amsterdam_Muiderpoort')
        self.assertEqual(len(station.positions), 2)

        position1 = TSStationPosition.get('nl.test_os01')
        self.assertIsNotNone(position1)
        position2 = TSStationPosition.get('nl.test_ssh01')
        self.assertIsNotNone(position2)

        # Delete the entry
        station.delete()
        station = TSStation.get('nl.test')
        self.assertEqual(station, None)

        position1 = TSStationPosition.get('nl.test_os01')
        self.assertIsNone(position1)
        position2 = TSStationPosition.get('nl.test_ssh01')
        self.assertIsNone(position2)

    def test_rest_interface(self):
        """
        Test REST capabilities
        i.e. Create, Read, Update and Delete over http interface
        """

        # with correct input a user must be created
        response = self.testapp.post('/atlas/admin',
                                     json.dumps({'realm': 'backoffice02@firstflamingo.com', 'token': '123456'}),
                                     headers={'Content-Type': 'application/json'})
        result = json.loads(response.body)
        username = result.get('username')
        admin = TSAdmin.get_by_id(int(username))
        self.assertEqual(admin.ha1, '37075c9a3dece9f4f56ede004a369a7d')
        admin.enabled_admin = True
        admin.put()

        # with a user credentials, an authorized request can be made
        path = '/atlas/station/nl.utt'
        challenge = self.testapp.put(path, status=401)
        authorization = auth_header('GET', path, challenge, admin)
        self.testapp.get(path, headers={'Authorization': authorization}, status=404)

        # Create entity with PUT
        dictionary = {
            'id': 'nl.utt',
            'names': ['Utrecht Terwijde', 'Terwijde'],
            'displayIndex': 1,
            'labelAngle': 60,
            'importance': 1,
            'wikiString': 'nl:Station Utrecht Terwijde',
            'openedString': '2003',
            'positions': [
                {'km': 4.398, 'route': 'nl.nrs01', 'lat': 52.3605540, 'lon': 4.9311113}
            ]}
        authorization = auth_header('PUT', path, challenge, admin)
        headers = {'Authorization': authorization, 'Content-Type': 'application/json'}
        response = self.testapp.put(path, json.dumps(dictionary), headers=headers)
        result = json.loads(response.body)
        object_id = result['id']
        created_object = TSStation.get(object_id)
        self.assertIsNotNone(created_object)
        self.assertEqual(created_object.importance, 1)
        self.assertEqual(created_object.name, 'Utrecht Terwijde')
        created_position = TSStationPosition.get('nl.utt_nrs01')
        self.assertIsNotNone(created_position)
        self.assertEqual(created_position.km, 4.398)

        # Read entity with GET
        authorization = auth_header('GET', path, challenge, admin)
        headers = {'Authorization': authorization, 'Accept': 'application/json'}
        response = self.testapp.get(path, headers=headers)
        result = json.loads(response.body)
        self.assertEqual(result, dictionary)

        # Modify object with PUT
        dictionary['importance'] = 3
        authorization = auth_header('PUT', path, challenge, admin)
        headers = {'Authorization': authorization, 'Content-Type': 'application/json',
                   'If-Unmodified-Since': rfc1123_from_utc(created_object.last_modified)}
        response = self.testapp.put(path, json.dumps(dictionary), headers=headers)
        result = json.loads(response.body)
        self.assertEqual(result, dictionary)

        changed_object = TSStation.get(object_id)
        self.assertEqual(changed_object.importance, 3)

        # Remove object with DELETE
        authorization = auth_header('DELETE', path, challenge, admin)
        self.testapp.delete(path, headers={'Authorization': authorization}, status=204)
        gone_object = TSStation.get(object_id)
        self.assertIsNone(gone_object)
        gone_position = TSStationPosition.get('nl.utt_nrs01')
        self.assertIsNone(gone_position)

    def test_positions(self):
        station = TSStation.new('nl.asd')
        station.names = ['Amsterdam']
        position = station.create_position('ol02')
        position.km = 1.234
        position.coordinate = (4.751, 52.222)
        station.put()
        position.put()
        self.assertEqual(len(station.positions), 1)
        self.assertIsNotNone(position)
        self.assertEqual(position.station_id, 'nl.asd')
        self.assertEqual(position.route_id, 'nl.ol02')
        self.assertEqual(position.km, 1.234)
        self.assertEqual(position.coordinate, (4.751, 52.222))
        self.assertEqual(position.station.name, 'Amsterdam')
