#!/usr/bin/env python
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
#  TAToolsHandler.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 06-Feb-13.
#

from google.appengine.ext import db
from google.appengine.api import memcache
import webapp2
import math, logging

from datetime   import datetime, timedelta

from ffe                import markup
from ffe.ffe_time       import mark_cet, utc_from_cet, minutes_from_string, string_from_minutes
from TASeries           import TASeries
from TAMission          import TAMission, round_mission_offset
from TAStop             import TAStop, StopStatuses
from TAScheduledPoint   import Direction

MENU_LIST = (('Home', '/console'),
             ('Series', '/console/series?page=1'),
             ('Missies zonder serie', '/console/missions?kind=orphans&page=1'),
             ('Stations', '/console/stations?page=1'),
             ('Rapportage', '/console/report'))

FIRST_HALF  = 0
SECND_HALF  = 1
ORD_LABEL   = ['Eerste', 'Tweede']

# URL Handlers

class RepatternHandler(webapp2.RequestHandler):
    
    def get(self):
        series =TASeries.get(self.request.get('series'))
        self.results = [{}, {}]
        self.analyzeStops()
        
        self.doc = ToolsDocument('Patroontijden voor serie %s' % series.name)
        
        form = markup.form('/tools/repattern', 'post')
        form.add(markup.input('hidden', 'series', self.request.get('series')))
        form.add(markup.heading(2, 'Heenrichting'))
        form.add(self.patternTimeTable(series, Direction.up))
        form.add(markup.heading(2, 'Terugrichting'))
        form.add(self.patternTimeTable(series, Direction.down))
        form.add(markup.input('submit', value='pas aan'))
        
        self.doc.main.add(form)
        self.response.out.write(self.doc.write())

    def post(self):
        series =TASeries.get(self.request.get('series'))
        self.doc = ToolsDocument('Aangepaste tijden voor serie %s' % series.name)
        processedObjects   = []
        processedPoints    = {}
        
        table = self.doc.add_table('changestable', ['Station', 'A', 'V', 'A', 'V', ''])
        for index in range(len(series.points)):
            point = series.points[index]
            oldTimes = point.scheduled_times
            upArrival = self.request.get('arr_%d_%d' % (Direction.up, index))
            upDeparture = self.request.get('dep_%d_%d' % (Direction.up, index))
            downArrival = self.request.get('arr_%d_%d' % (Direction.down, index))
            downDeparture = self.request.get('dep_%d_%d' % (Direction.down, index))
            newTimes = (minutes_from_string(upArrival),
                        minutes_from_string(upDeparture),
                        minutes_from_string(downArrival),
                        minutes_from_string(downDeparture))
            
            row = table.add_row()
            row.add_to_cell(0, point.stationName)
            row.add_to_cell(1, upArrival)
            row.add_to_cell(2, upDeparture)
            row.add_to_cell(3, downArrival)
            row.add_to_cell(4, downDeparture)
            
            if oldTimes != newTimes:
                point.scheduled_times = newTimes
                processedPoints[point.id] = point
                processedObjects.append(point)
                row.add_to_cell(5, 'aangepast')

        series.cache_set()
        memcache.set_multi(processedPoints, namespace='TAScheduledPoint')
        db.put(processedObjects)
        self.response.out.write(self.doc.write())

    def patternTimeTable(self, series, direction):
        table = markup.HTMLTable('timetable_%d' % direction, ['Station', 'A', 'V', 'meting', '#', 'delta', 'A', 'V'])
        indexes = range(len(series.points))
        if direction == Direction.down: indexes.reverse()
        for index in indexes:
            point = series.points[index]
            station = point.station
            planArrival, planDeparture = point.times_in_direction(direction)
            row = table.add_row()
            row.add_to_cell(0, station.name)
            row.add_to_cell(1, string_from_minutes(planArrival))
            row.add_to_cell(2, string_from_minutes(planDeparture))
            stationDict = self.results[direction].get(station.id, None)
            if stationDict == None:
                departure, count = ('-', '-')
                delta = 0
            else:
                departure, count = mostCommonItem(stationDict['v'])
                delta = departure - planDeparture
                departure = string_from_minutes(departure)
            row.add_to_cell(3, departure)
            row.add_to_cell(4, count)
            row.add_to_cell(5, delta)
            row.add_to_cell(6, markup.input('text', 'arr_%d_%d' % (direction, index), string_from_minutes(planArrival + delta), size=4))
            row.add_to_cell(7, markup.input('text', 'dep_%d_%d' % (direction, index), string_from_minutes(planDeparture + delta), size=4))
        return table

    def analyzeStops(self):
        series_id = self.request.get('series')
        query = db.Query(TAArchivedMission).filter('series_id =', series_id)
        for mission in query.fetch(50):
            if mission.up: direction = Direction.up
            else: direction = Direction.down
            for stop in mission.stopsList:
                stopKey = stop.station_id
                if stop.status == StopStatuses.planned:
                    departureHist = self.histogram(direction, stopKey, 'v')
                    difference = utc_from_cet(stop.departure) - correctedOffsetUTC(mission)
                    self.addDataToHistogram(departureHist, difference.seconds // 60)
                    delayHist = self.histogram(direction, stopKey, 'dv')
                    self.addDataToHistogram(delayHist, int(stop.delay_dep))
                    platformHist = self.histogram(direction, stopKey, 'p')
                    self.addDataToHistogram(platformHist, stop.platform)

    def stopDictionary(self, direction, stopKey):
        dictionary = self.results[direction].get(stopKey, None)
        if dictionary == None:
            dictionary = dict()
            self.results[direction][stopKey] = dictionary
        return dictionary
    
    def histogram(self, direction, stopKey, dataKey):
        stopDictionary = self.stopDictionary(direction, stopKey)
        dictionary = stopDictionary.get(dataKey, None)
        if dictionary == None:
            dictionary = dict()
            stopDictionary[dataKey] = dictionary
        return dictionary

    def addDataToHistogram(self, histogram, key):
        histogram[key] = histogram.get(key, 0) + 1


class ReoffsetHandler(webapp2.RequestHandler):
    
    tableTitles = ('tijd', 'aantal', 'perc.')
    tableFormat = (':%02d', '%d', '%.1f%%')

    def get(self):
        series =TASeries.get(self.request.get('series'))
        
        self.doc = ToolsDocument('Herschik offsets serie %s' % series.name)
        self.writeReport(series)
        self.response.out.write(self.doc.write())
    
    def post(self):
        series              = TASeries.get(self.request.get('series'))
        self.deltaOffset    = [int(self.request.get('offset_up')), int(self.request.get('offset_down'))]
        self.round          = [int(self.request.get('round_up')), int(self.request.get('round_down'))]
        self.processedObjects   = []
        self.processedMissions  = {}
        self.processedPoints    = {}
            
        self.doc = ToolsDocument('Aangepaste offsets serie %s' % series.name)
        self.doc.main.add(markup.heading(2, 'Aangepaste patroontijden'))
        self.processPoints(series)
        self.doc.main.add(markup.heading(2, 'Aangepaste offsettijden'))
        table = self.doc.add_table('adapted_missions', ['Missie', 'Offset'])
        self.processMissions(series.all_mission_ids(Direction.up), Direction.up, table)
        self.processMissions(series.all_mission_ids(Direction.down), Direction.down, table)
        series.cache_set()
        self.saveChanges()
        
#        self.writeReport(series)
        self.response.out.write(self.doc.write())

    def writeReport(self, series):
        self.departure = [series.first_point.upDeparture, series.last_point.downDeparture]
        self.startStation = [series.first_point.stationName, series.last_point.stationName]
        self.foundOffset = [None, None]

        self.doc.main.add(markup.heading(2, 'Heenrichting'))
        self.analyzeOffset(series.all_mission_ids(Direction.up))
        self.reportOffset(FIRST_HALF, Direction.up)
        self.reportOffset(SECND_HALF, Direction.up)

        self.doc.main.add(markup.heading(2, 'Terugrichting'))
        self.analyzeOffset(series.all_mission_ids(Direction.down))
        self.reportOffset(FIRST_HALF, Direction.down)
        self.reportOffset(SECND_HALF, Direction.down)

        if self.foundOffset[Direction.up] or self.foundOffset[Direction.down]:
            self.doc.main.add(markup.heading(2, 'Aanpassen'))
            self.proposeChanges()

    def analyzeOffset(self, missionIDs):
        self.offset = [None, None]
        self.data=[[], []]
        
        firstHalfHist = dict()
        firstHalfItems = 0
        secondHalfHist = dict()
        secondHalfItems = 0
        for missionID in missionIDs:
            mission = TAMission.get(missionID)
            num = mission.number
            if bool(num % 2): num -= 1
            key = mission.offset.minute
            if bool(num % 4):
                firstHalfHist[key] = firstHalfHist.get(key, 0) + 1
                firstHalfItems += 1
            else:
                secondHalfHist[key] = secondHalfHist.get(key, 0) + 1
                secondHalfItems += 1
        self.generateData(FIRST_HALF, firstHalfHist, firstHalfItems)
        self.generateData(SECND_HALF, secondHalfHist, secondHalfItems)

    def generateData(self, halfHour, histogram, count):
        maxFrequency = 0
        for key, value in histogram.iteritems():
            self.data[halfHour].append((int(key), value, 100.0 * value/count))
            if value > maxFrequency:
                maxFrequency = value
                self.offset[halfHour] = int(key)

    def reportOffset(self, halfHour, direction):
        if self.offset[halfHour] != None:
            self.doc.main.add(markup.heading(3, '%s halfuur :%02d' % (ORD_LABEL[halfHour], self.offset[halfHour])))
            table = self.doc.add_table('table_%d' % (2 * direction + halfHour), self.tableTitles, self.tableFormat)
            table.fill_data(self.data[halfHour])
            departure = self.offset[halfHour] + self.departure[direction]
            if departure >= 60:
                departure -= 60
                self.offset[halfHour] -= 60
            self.doc.add_paragraph('Vertrek uit %s: %d + %d = :%02d' %
                              (self.startStation[direction], self.offset[halfHour], self.departure[direction], departure))
            if self.foundOffset[direction] == None or self.offset[halfHour] < self.foundOffset[direction]:
                self.foundOffset[direction] = self.offset[halfHour]

    def proposeChanges(self):
        table = markup.HTMLTable('submit_table', ['', 'Offset', 'Afronden'])
        form = markup.form('/tools/reoffset', 'post')
        form.add(markup.input('hidden', 'series', self.request.get('series')))
        form.add(table)
        self.doc.main.add(form)
        
        row = table.add_row()
        row.add_to_cell(0,'heen')
        row.add_to_cell(1, markup.input('text', 'offset_up', str(self.foundOffset[Direction.up]), size=6))
        row.add_to_cell(2, markup.input('text', 'round_up', '3', size=6))
        
        row = table.add_row()
        row.add_to_cell(0,'terug')
        row.add_to_cell(1, markup.input('text', 'offset_down', str(self.foundOffset[Direction.down]), size=6))
        row.add_to_cell(2, markup.input('text', 'round_down', '3', size=6))
        
        row = table.add_row()
        row.add_to_cell(0, markup.input('submit', value='pas aan'))

    def processPoints(self,series):
        table = self.doc.add_table('adapted_schedule', ['Station', 'Heen', 'Terug'])
        for point in series.points:
            # Change arrival and departure times:
            oldUp, oldDown = point.times_strings
            point.upArrival += self.deltaOffset[Direction.up]
            point.upDeparture += self.deltaOffset[Direction.up]
            point.downArrival += self.deltaOffset[Direction.down]
            point.downDeparture += self.deltaOffset[Direction.down]
            newUp, newDown = point.times_strings
            
            # Add point to queue for saveChanges
            self.processedPoints[point.id] = point
            self.processedObjects.append(point)
            
            # Report the changes:
            row = table.add_row()
            row.add_to_cell(0, point.stationName)
            row.add_to_cell(1, '[%s] %s [%s]' % (oldUp, change_string(self.deltaOffset[Direction.up]), newUp))
            row.add_to_cell(2, '[%s] %s [%s]' % (oldDown, change_string(self.deltaOffset[Direction.down]), newDown))

    def processMissions(self, missionIDs, direction, table):
        if self.deltaOffset[direction]:
            for missionID in missionIDs:
                # Change mission offset time:
                mission = TAMission.get(missionID)
                oldOffset = datetime(2002, 2, 2).replace(hour=mission.offset.hour, minute=mission.offset.minute)
                newOffset = round_mission_offset(oldOffset - timedelta(minutes=self.deltaOffset[direction]), self.round[direction])
                mission.offset = newOffset.time()
                
                # Add mission to queue for saveChanges
                self.processedMissions[missionID] = mission
                self.processedObjects.append(mission)
                
                # Report the changes:
                row = table.add_row()
                row.add_to_cell(0, missionID)
                row.add_to_cell(1, '%s %s %s' % (oldOffset.strftime('%H:%M'),
                                                 change_string(-self.deltaOffset[direction]),
                                                 newOffset.strftime('%H:%M')))

    def saveChanges(self):
        memcache.set_multi(self.processedPoints, namespace='TAScheduledPoint')
        memcache.set_multi(self.processedMissions, namespace='TAMission')
        db.put(self.processedObjects)


# HTML Document

class ToolsDocument(markup.HTMLDocument):
    
    def __init__(self, title, language='en'):
        markup.HTMLDocument.__init__(self, title, language)
        
        #Stylesheet
        style_element = markup.link('stylesheet', '/web/style.css')
        style_element.set_attribute('type', 'css')
        style_element.set_attribute('media', 'screen')
        self.head.add(style_element)
        
        #Header
        self.header = markup.XMLElement('header')
        self.header.add(markup.user_id())
        self.header.add(markup.heading(1, title))
        self.body.add(self.header)
        
        #Paper with two columns: sidebar and main
        paper = markup.div('paper')
        self.main = markup.div('main_content')
        paper.add(self.main)
        self.sidebar = markup.element_with_id('aside', 'sidebar')
        self.sidebar.add(markup.main_menu(MENU_LIST))
        paper.add(self.sidebar)
        paper.add(markup.div('pushbottom'))
        self.body.add(paper)
        
        #Footer
        self.footer = markup.XMLElement('footer')
        self.footer.add(markup.paragraph('First Flamingo Enterprise B.V.'))
        self.body.add(self.footer)
    
    def add_paragraph(self, paragraphText):
        self.main.add(markup.paragraph(paragraphText))
    
    def add_reference(self, href, content):
        paragraph = markup.paragraph('')
        paragraph.add(markup.anchor(href, content))
        self.main.add(paragraph)
    
    def add_table(self, name, columnTitles, format=None):
        table = markup.HTMLTable(name, columnTitles)
        if format != None: table.format = format
        self.main.add(table)
        return table
    
    def add_page_navigator(self, currentPage, lastPage, urlFormat):
        self.main.add(markup.page_navigator(currentPage, lastPage, urlFormat))


# Helper functions

def change_string(number):
    if number < 0: return '- %d =' % -number
    else: return'+ %d =' % number
    
def mostCommonItem(histogram):
    maxValue = 0
    foundKey = None
    for key, value in histogram.iteritems():
        if value > maxValue:
            foundKey = key
            maxValue = value
    return (foundKey, maxValue)

def correctedOffsetUTC(archivedMission):
    ''' Replaces the offset time as stored in the TAArchivedMission with that from the corresponding TAMission,
        while retaining the date.
        '''
    originalMission = TAMission.get('%s.%d' % (archivedMission.country, archivedMission.baseNumber))
    offsetCET = mark_cet(datetime.combine(archivedMission.offset_CET.date(), originalMission.offset))
    return utc_from_cet(offsetCET)


# WSGI Application

app = webapp2.WSGIApplication([('/tools/repattern.*', RepatternHandler),
                               ('/tools/reoffset.*', ReoffsetHandler)
                               ], debug=True)
