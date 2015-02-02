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
#  TSRoute.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 13-May-14.
#

"""TSRoute represents a railway route and syncs with ATLRoute in Rail Atlas"""

import logging
import re
from google.appengine.ext   import ndb
from ffe.rest_resources import PublicResource


class TSRoute(PublicResource):
    url_name = 'route'
    wiki_string = ndb.StringProperty(indexed=False)
    identifier_regex = re.compile('([a-z]{2})\.([a-z]{2,4}[0-9]{1,2})$')
