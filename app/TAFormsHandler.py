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
#  TAFormsHandler.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 27-Jan-2013.
#

import webapp2, logging
from google.appengine.ext   import db

from TASeries        import TASeries
from TAMission       import TAMission

class SeriesHandler(webapp2.RequestHandler):
    
    def post(self):
        seriesID = self.request.get('series')
        direction = self.request.get('direction')
        logging.info('Handle form for series %s (%s).' % (seriesID, direction))
        changedObjects = []
        series = TASeries.get(seriesID)
        if direction != 'down':
            missions = series.up_missions
        else:
            missions = series.down_missions
        for mission in missions:
            missionCode = mission.code
            offsetString = self.request.get('offset_%s' % missionCode)
            if offsetString:
                mission.offset_string = offsetString
            odIDsString = self.request.get('odids_%s' % missionCode)
            if odIDsString:
                mission.odIDs_dictionary = dictionaryFromODIDs(odIDsString)
            if mission.needs_datastore_put:
                logging.info('changed mission: %s' % missionCode)
                mission.needs_datastore_put = False
                mission.cache_set()
                changedObjects.append(mission)
        db.put(changedObjects)
        self.redirect('/console/missions?kind=pattern&series=%s&direction=%s&changes=%d' % (seriesID, direction, len(changedObjects)))

def dictionaryFromODIDs(odIDsString):
    dictionary = {}
    items = odIDsString.split(',')
    for item in items:
        dayString, value = item.split(':')
        fromID, toID = value.split('-')
        fromID = fromID.strip()
        if fromID == 'None': fromID = None
        toID = toID.strip()
        if toID == 'None': toID = None
        dictionary[keyString(dayString)] = [fromID, toID]
    return dictionary

def keyString(dayString):
    day=dayString.strip()
    if day == 'ma':
        return '0'
    elif day == 'di':
        return '1'
    elif day == 'wo':
        return '2'
    elif day == 'do':
        return '3'
    elif day == 'vr':
        return '4'
    elif day == 'za':
        return '5'
    elif day == 'zo':
        return '6'
    elif day == 'normaal':
        return 'd'


# WSGI Application

app = webapp2.WSGIApplication([('/forms/series', SeriesHandler)
                               ], debug=True)

