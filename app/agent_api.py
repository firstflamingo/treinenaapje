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
#  agent_api.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 22-May-14.
#

"""agent_api contains the API for """

import webapp2
from ffe.rest_interface import AgentHandler
from TSStationAgent import TSStationAgent


class StationHandler(AgentHandler):
    resource_class = TSStationAgent


# ====== WSGI Application ==========================================================================


AGENT_URL_SCHEMA = [('/agent/station.*', StationHandler)]
app = webapp2.WSGIApplication(AGENT_URL_SCHEMA, debug=True)
