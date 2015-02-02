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
#  TAManager.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 21-Feb-13.
#

import webapp2
import logging

from datetime               import timedelta
from google.appengine.ext   import db
from google.appengine.api   import memcache, taskqueue

from ffe                    import config
from ffe.gae                import issue_tasks, task_name
from ffe.ffe_time           import now_utc
from TSStation              import TSStation
from TASeries               import TASeries
from TAMission              import TAMission


class TARequestHandler(webapp2.RequestHandler):

    _instruction = None

    def get(self):

        if self.instruction == 'create_avt_tasks':
            self.create_and_issue_tasks(TSStation, config.STATION_AVT_DURATION, 'avt')

        elif self.instruction == 'update_stations':
            TSStation.update_stations()

        elif self.instruction == 'new_day':
            self.create_and_issue_tasks(TASeries, config.SERIES_CONSOLIDATION_DURATION, 'new_day')

        elif self.instruction == 'remove_orphans':
            self.remove_orphans()

    @staticmethod
    def create_and_issue_tasks(target_class, period, instruction):
        tasks = []
        ids = target_class.active_ids()
        if ids:
            issue_time = now_utc() + timedelta(seconds=config.WAIT_BEFORE_FIRST_TASK)
            period *= 60.0
            eta_delta = period / len(ids)
            for identifier in ids:
                url = '%s/%s' % (target_class.agent_url, identifier)
                logging.info('Create task for %s at %s UTC' % (url, issue_time.strftime('%H:%M:%S')))
                task = taskqueue.Task(name=task_name(issue_time, instruction),
                                      url=url,
                                      params={'inst':instruction},
                                      eta=issue_time)
                tasks.append(task)
                issue_time += timedelta(seconds=eta_delta)
        issue_tasks(tasks)

    @staticmethod
    def remove_orphans():
        mission_keys = db.Query(TAMission, keys_only=True).filter('series_id =', 'orphan').fetch(1000)
        mission_ids = []
        for key in mission_keys:
            mission_ids.append(key.name())
        logging.info('Remove %d orphan missions' % len(mission_keys))
        memcache.delete_multi(mission_ids, namespace='TAMission')
        db.delete(mission_keys)

    @property
    def instruction(self):
        if self._instruction is None:
            comps = self.request.path.split('/')
            if len(comps) == 3:
                self._instruction = comps[2]
        return self._instruction


# WSGI Application

URL_SCHEMA = [('/TAManager.*', TARequestHandler)]
app = webapp2.WSGIApplication(URL_SCHEMA, debug=True)
