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
#  atlas_api.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 12-May-14.
#

"""atlas_api contains the API for the REST-full interface for Rail Atlas objects"""

import webapp2
from ffe.rest_interface import RestHandler
from TSAdmin import TSAdmin
from TSStation import TSStation
from TSJunction import TSJunction
from TSRoute import TSRoute


class StationHandler(RestHandler):
    resource_class = TSStation
    user_class = TSAdmin


class JunctionHandler(RestHandler):
    resource_class = TSJunction
    user_class = TSAdmin


class RouteHandler(RestHandler):
    resource_class = TSRoute
    user_class = TSAdmin


class AtlasAdminHandler(RestHandler):
    allows_anonymous_post = True
    resource_class = TSAdmin
    user_class = TSAdmin


# ====== WSGI Application ==========================================================================


ATLAS_URL_SCHEMA = [('/atlas/station.*', StationHandler),
                    ('/atlas/junction.*', JunctionHandler),
                    ('/atlas/route.*', RouteHandler),
                    ('/atlas/admin.*', AtlasAdminHandler)]
app = webapp2.WSGIApplication(ATLAS_URL_SCHEMA, debug=True)
