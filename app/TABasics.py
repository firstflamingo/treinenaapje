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
#  TABasics.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 09-Oct-12.
#

import webapp2, json, random

import logging

from ffe.markup             import XMLDocument
from ffe.ffe_time           import utc_from_cet, string_from_cet
from ffe.gae                import task_name
from google.appengine.ext   import db
from google.appengine.api   import memcache, taskqueue


class JSONProperty(db.TextProperty):
    
    def validate(self, value):
        return value
    
    def get_value_for_datastore(self, model_instance):
        dictionary = super(JSONProperty, self).get_value_for_datastore(model_instance)
        serialized = json.dumps(dictionary)
        return db.Text(serialized)
    
    def make_value_from_datastore(self, value):
        dictionary = json.loads(str(value))
        return super(JSONProperty, self).make_value_from_datastore(dictionary)


class TAModel(db.Model):

    # Object lifecycle:
    @classmethod
    def new(cls, id=None, code=None, country='nl'):
        if not id:
            id = '%s.%s' % (country, code)
        logging.info('Create new %s %s.' % (cls.__name__, id))
        self = cls(key_name=id)
        self.awake_from_create()
        self.cache_set()
        return self

    @classmethod
    def get(cls, id=None, code='', country='nl', class_name=None, create=False, now=None):
        if not class_name:
            class_name = cls.__name__
        if not id:
            id = '%s.%s' % (country, code)
        self = memcache.get(id, namespace=class_name)
        if not self:
            self = db.get(db.Key.from_path(class_name, id))
            if self:
                self.awake_from_fetch(now)
                self.cache_set()
            elif create:
                self = cls.new(id)
        return self

    def awake_from_create(self):
        pass

    def awake_from_fetch(self, now):
        pass

    # Finding instances
    @classmethod
    def all_ids(cls):
        """
        Provides a list with ids for all instances of this class, in alphabetical order
        :rtype : list
        """
        memcache_key = '%s_ids' % cls.__name__
        ids_list = memcache.get(memcache_key)
        if not ids_list:
            ids_list = []
            for key in db.Query(cls, keys_only=True).fetch(1000):
                ids_list.append(key.name())
            memcache.set(memcache_key, ids_list)
        return ids_list

    @classmethod
    def active_ids(cls):
        """
        Provides a list of ids for instances of the class, with a class specific filter and order
        The default implementation returns all_ids()
        :rtype : list
        """
        return cls.all_ids()

    @classmethod
    def reset_ids(cls):
        memcache.delete('%s_ids' % cls.__name__)

    @classmethod
    def dictionary_from_list(cls, the_list):
        dictionary = {}
        for object in the_list:
            dictionary[object.id] = object
        return dictionary

    @classmethod
    def objects_dictionary(cls):
        return cls.dictionary_from_list(db.Query(cls).fetch(1000))

    @classmethod
    def paginatedObjects(cls, page=1, length=20):
        array = []
        end = page * length
        start = end - length
        for id in cls.all_ids()[start:end]:
            array.append(cls.get(id))
        return array
    
    @classmethod
    def xml_catalog(cls):
        document = XMLDocument(cls.__name__)
        for object in db.Query(cls).fetch(1000):
            document.root.add(object.xml)
        return document

    @property
    def id(self):
        return self.key().name()

    @property
    def country(self):
        return self.id.split('.')[0]

    @property
    def code(self):
        return self.id.split('.')[1]

    @property
    def url(self):
        return '/%s/%s' % (self.__class__.__name__, self.id)
    
    # Managing cache:
    def cache_set(self):
        memcache.set(self.id, self, namespace=self.key().kind())

    def put(self):
        db.Model.put(self)
        self.cache_set()

    def delete(self):
        memcache.delete(self.id, namespace=self.key().kind())
        db.Model.delete(self)

    def instruction_task(self, url, instruction, issue_time_cet, expected=None, random_s=False):
        logging.info('Schedule %s-update for %s at %s CET' % (instruction, url, issue_time_cet.strftime('%H:%M:%S')))
        params = {'inst': instruction, 'sender': self.id}
        if expected is not None:
            params['expected'] = string_from_cet(expected)
        label = '%s_%s' % (instruction, self.code)
        issue_time = utc_from_cet(issue_time_cet)
        if random_s:
            seconds = int(59.99 * random.random())
            issue_time = issue_time.replace(second=seconds)
            name = issue_time.strftime('%d_%H%M_xx_') + label
        else:
            name = issue_time.strftime('%d_%H%M_%S_') + label
        return taskqueue.Task(name=name, url=url, params=params, eta=issue_time)

    def stop_task(self, stop, issue_time_cet):
        label = 'fwd_' + self.code
        url = '/TAMission/%s' % stop.mission_id
        payload = json.dumps(stop.repr)
        logging.info('Forward stop to %s at %s CET' % (stop.mission_id, issue_time_cet.strftime('%H:%M:%S')))
        issue_time = utc_from_cet(issue_time_cet)
        return taskqueue.Task(name=task_name(issue_time, label),
                              url=url,
                              eta=issue_time,
                              payload=payload,
                              headers={'Content-Type': 'application/json'})


class TAResourceHandler(webapp2.RequestHandler):
    
    _resource       = None
    resourceClass   = None

    def get(self):
        format = self.request.get('format')
        if format == 'xml':
            self.response.content_type = 'application/xml'
            self.response.out.write(self.xml)
        else:
            self.response.content_type = 'application/json'
            self.response.out.write(self.json)

    def post(self):
        type = self.request.content_type
        if type == 'application/x-www-form-urlencoded':
            self.perform()
        elif type == 'application/json':
            dictionary = json.loads(self.request.body)
            self.receive(dictionary)

    def perform(self):
        pass

    def receive(self, dictionary):
        pass

    @property
    def resource(self):
        if self._resource != None: return self._resource
        if self.resourceClass:
            id = self.resource_id
            if id: self._resource = self.resourceClass.get(id)
            return self._resource

    @property
    def resource_id(self):
        comps = self.request.path.split('/')
        if len(comps) == 3:
            return comps[2]

    @property
    def json(self):
        if self.resource:
            return json.dumps(self.resource.repr)
        else:
            return json.dumps(self.resourceClass.all_ids())

    @property
    def xml(self):
        if self.resource:
            return self.resource.xml_document.write(lf=True)
        else:
            return self.resourceClass.xml_catalog().write(lf=True)
