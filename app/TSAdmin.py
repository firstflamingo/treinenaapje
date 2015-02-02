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
#  TSAdmin.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 30-Apr-14.
#

"""TSAdmin is a subclass of TSuser, defining administrator users of the ffe-backoffice-02 domain"""

import logging
from google.appengine.ext   import ndb
from ffe.rest_resources import User


class TSAdmin(User):
    url_name = 'admin'
    enabled_admin = ndb.BooleanProperty(default=False)
    realm = 'backoffice02@firstflamingo.com'

    @property
    def has_admin_privileges(self):
        return self.enabled_admin
