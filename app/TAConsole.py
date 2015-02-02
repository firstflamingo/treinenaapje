#!/usr/bin/env python
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
#  TAConsole.py
#  firstflamingo/treinenaapje
#
#  Created by Berend Schotanus on 09-Oct-12.
#

import webapp2, json
import math, logging

from ffe                import markup
from ffe.gae            import counter_dict

from TAScheduledPoint   import Direction
from TSStation          import TSStation
from TASeries           import TASeries
from TAMission          import TAMission, MissionStatuses

MENU_LIST = (('Home', '/console'),
             ('Stations', '/console/stations'),
             ('Series', '/console/series'))


# URL Handlers

class ConsoleHandler(webapp2.RequestHandler):

    def get(self):
        document = ConsoleDocument('Treinenaapje Console')
        nrOfStations = len(TSStation.all_ids())
        nrOfSeries = len(TASeries.all_ids())

        if nrOfStations == 0:
            document.add_paragraph('Er zijn geen stations beschikbaar.')

        elif nrOfSeries == 0:
            document.add_paragraph('Er zijn geen series beschikbaar.')
            form = markup.form('/TASeries', 'post')
            form.add(markup.input('hidden', 'inst', 'fetch'))
            form.add(markup.input('submit', value='Series laden'))
            document.main.add(form)

        else:
            document.add_paragraph('Er zijn %d stations en %d series' % (nrOfStations, nrOfSeries))

        table = document.add_table('statistics_table', ['naam', 'aantal'])
        row = table.add_row()
        row.add_to_cell(0, 'stations')
        row.add_to_cell(1, nrOfStations)
        row = table.add_row()
        row.add_to_cell(0, 'series')
        row.add_to_cell(1, nrOfSeries)
        for key, value in counter_dict().items():
            row = table.add_row()
            row.add_to_cell(0, key)
            row.add_to_cell(1, str(value))

        self.response.out.write(document.write())


class StationsHandler(webapp2.RequestHandler):

    def get(self):
        nr_of_stations = len(TSStation.all_ids())
        stations_per_page = 20
        current_page = int(self.request.get('page', '1'))
        last_page = int(math.ceil(float(nr_of_stations) / stations_per_page))

        document = ConsoleDocument('Stations')
        if nr_of_stations == 0:
            document.add_paragraph('Er zijn geen stations beschikbaar.')
        else:
            document.add_paragraph('%d stations beschikbaar.' % nr_of_stations)
            table = document.add_table('stations_table', ['code', 'naam', 'geopend', 'wiki', 'requests'])
            for station in TSStation.paginated_objects(current_page, stations_per_page):
                station_code = station.code
                row = table.add_row()
                row.add_to_cell(0, station_code)
                row.add_link_to_cell(1, '/console/departures?station=%s' % station.id_, station.name)
                row.add_to_cell(2, station.opened_string)
                wiki = station.wiki_link
                if wiki:
                    row.add_link_to_cell(3, wiki, 'wiki')
                row.add_to_cell(4, '%d' % station.agent.nr_of_fetches)

            document.add_page_navigator(current_page, last_page, '/console/stations?page=%d')
        self.response.out.write(document.write())


class DeparturesHandler(webapp2.RequestHandler):

    def get(self):
        station = TSStation.get(self.request.get('station'))
        document = ConsoleDocument(station.name)
        table = document.add_table('departures_table', ['s', 'V', 'dV', 'treinnr.', 'bestemming', 'perron'])
        for the_stop in station.agent.sorted_stops:
            row = table.add_row()
            row.add_to_cell(0, the_stop.status)
            row.add_to_cell(1, the_stop.departure_string)
            row.add_to_cell(2, delay_string(the_stop.delay_dep))
            mission_code = the_stop.mission_id.split('.')[1]
            row.add_link_to_cell(3, '/console/schedule?mission=%s' % the_stop.mission_id, mission_code)
            row.add_to_cell(4, the_stop.destination)
            if the_stop.platformChange:
                row.add_to_cell(5, '%s <<' % the_stop.platform)
            else:
                row.add_to_cell(5, the_stop.platform)

        form = markup.form('/agent/station/%s' % station.id_, 'post')
        form.add(markup.input('hidden', 'inst', 'console'))
        form.add(markup.input('hidden', 'redirect', '/console/departures?station=%s' % station.id_))
        form.add(markup.input('submit', value='Verversen'))
        document.main.add(form)

        self.response.out.write(document.write())


class SeriesHandler(webapp2.RequestHandler):

    _series = None

    def get(self):

        if self.series is None:
            nrOfSeries = len(TASeries.all_ids())
            seriesPerPage = 20
            currentPage = int(self.request.get('page', '1'))
            lastPage = int(math.ceil(float(nrOfSeries) / seriesPerPage))

            document = ConsoleDocument('Treinseries')
            if nrOfSeries == 0:
                document.add_paragraph('Er zijn geen treinseries beschikbaar')
                document.add_reference('/TASeries?inst=fetch', 'Klik om treinseries op te halen')
            else:
                document.add_paragraph('%d treinseries beschikbaar.' % nrOfSeries)
                table = document.add_table('stations_table', ['serie', 'van', 'naar', 'actief', 'tools'])
                for series in TASeries.paginatedObjects(currentPage, seriesPerPage):
                    row = table.add_row()
                    row.add_link_to_cell(0, '/console/series?id=%s' % series.id, series.name)
                    row.add_to_cell(1, series.origin)
                    row.add_to_cell(2, series.destination)
                    number = series.nr_of_missions
                    if number > 0:
                        cell_text = '%d treinen' % number
                    else:
                        cell_text = '-'
                    row.add_link_to_cell(3, '/console/missions?kind=active&series=%s' % series.id, cell_text)
                    row.add_link_to_cell(4, '/console/missions?kind=pattern&series=%s&direction=up' % series.id, 'H')
                    row.add_to_cell(4, '|')
                    row.add_link_to_cell(4, '/console/missions?kind=pattern&series=%s&direction=down' % series.id, 'T')
                    row.add_to_cell(4, '|')
                    row.add_link_to_cell(4, '/tools/reoffset?series=%s' % series.id, 'O')
                    row.add_to_cell(4, '|')
                    row.add_link_to_cell(4, '/TASeries/%s?format=xml' % series.id, 'X')
                document.add_page_navigator(currentPage, lastPage, '/console/series?page=%d')
            self.response.out.write(document.write())

        else:
            document = ConsoleDocument('Serie %s' % self.series.name)
            url = '/TASeries/%s' % self.series.id
            form = markup.form(url, 'post')
            form.add(markup.input('hidden', 'inst', 'fetch'))
            form.add(markup.input('submit', value='Serie opnieuw laden'))
            document.main.add(form)

            if self.series.points:
                form = markup.form('/some_url', 'post')
                form.add(markup.input('hidden', 'series', self.series.id))
                form.add(markup.input('text', 'van', self.series.first_point.station_code, size=4))
                form.add(markup.input('text', 'naar', self.series.last_point.station_code, size=4))
                form.add(markup.input('submit', value='Tijden checken'))
                document.main.add(form)

            table = document.add_table('up_timetable', ['km', 'station', 'A', 'V', 'perron', 'tools'])
            for point in self.series.points:
                row = table.add_row()
                row.add_to_cell(0, '%.1f' % point.km)
                row.add_to_cell(1, self.series.name_for_point(point))
                if point.upArrival != point.upDeparture:
                    row.add_to_cell(2, string_from_minutes(point.upArrival))
                row.add_to_cell(3, string_from_minutes(point.upDeparture))
                row.add_to_cell(4, point.platform_string(Direction.up))

                form = markup.form(url, 'post')
                form.add(markup.input('hidden', 'inst', 'delete_point'))
                form.add(markup.input('hidden', 'sender', point.station_id))
                form.add(markup.input('submit', value='Verwijder'))
                row.add_to_cell(5, form)

            table = document.add_table('down_timetable', ['km', 'station', 'A', 'V', 'perron'])
            for point in reversed(self.series.points):
                row = table.add_row()
                row.add_to_cell(0, '%.1f' % point.km)
                row.add_to_cell(1, point.stationName)
                if point.downArrival != point.downDeparture:
                    row.add_to_cell(2, string_from_minutes(point.downArrival))
                row.add_to_cell(3, string_from_minutes(point.downDeparture))
                row.add_to_cell(4, point.platform_string(Direction.down))

            self.response.out.write(document.write())

    @property
    def series(self):
        if self._series is None:
            series_id = self.request.get('id')
            if series_id:
                self._series = TASeries.get(series_id)
        return self._series

class MissionsHandler(webapp2.RequestHandler):

    _series = None

    def get(self):
        kind = self.request.get('kind')
        if not self.series:
            self.response.out.write('Serie niet gevonden')
            return

        if kind == 'active':
            document = ConsoleDocument('Serie %s' % self.series.name)
            document.main.add(markup.heading(2, 'Heenrichting'))
            document.main.add(self.overviewTable(self.series.current_up_missions))
            document.main.add(markup.heading(2, 'Terugrichting'))
            document.main.add(self.overviewTable(self.series.current_down_missions))
            self.response.out.write(document.write())

        if kind == 'all':
            document = ConsoleDocument('Serie %s' % self.series.name)
            document.main.add(markup.heading(2, 'Heenrichting'))
            document.main.add(self.overviewTable(self.series.up_missions))
            document.main.add(markup.heading(2, 'Terugrichting'))
            document.main.add(self.overviewTable(self.series.down_missions))
            self.response.out.write(document.write())

        elif kind == 'pattern':
            direction = self.request.get('direction')
            up = direction != 'down'
            if up:
                label = 'heen'
                reverse = 'down'
                reverse_label = 'toon terugrichting'
                origin = self.series.first_point.stationName
                destination = self.series.last_point.stationName
                missions = self.series.up_missions
            else:
                label = 'terug'
                reverse = 'up'
                reverse_label = 'toon heenrichting'
                origin = self.series.last_point.stationName
                destination = self.series.first_point.stationName
                missions = self.series.down_missions

            document = ConsoleDocument('Serie %s (%s)' % (self.series.name, label))
            changes = self.request.get('changes')
            if changes: document.add_paragraph('%s missies zijn aangepast.' % changes)
            document.add_paragraph('%d treinen in serie %s %s - %s' % (self.series.nr_of_missions, self.series.name, origin, destination))
            document.add_reference('/console/missions?kind=pattern&series=%s&direction=%s' % (self.series.id, reverse),reverse_label)

            url = '/TASeries/%s' % self.series.id
            form1 = markup.form(url, 'post')
            form1.add(markup.input('hidden', 'inst', 'optimize_odids'))
            form1.add(markup.input('submit', value='Optimaliseer waarden'))
            document.main.add(form1)

            form2 = markup.form('/forms/series', 'post')
            form2.add(markup.input('hidden', 'series', self.series.id))
            form2.add(markup.input('hidden', 'direction', self.request.get('direction')))
            table = markup.HTMLTable('missions_table', ['Trein', 'Offset', 'Bediende traject'])
            form2.add(table)
            form2.add(markup.input('submit', value='Wijzigen'))
            document.main.add(form2)
            for mission in missions:
                row = table.add_row()
                row.add_to_cell(0, mission.number)
                row.add_to_cell(1, markup.input('text', 'offset_%d' % mission.number, mission.offset_string, size=6))
                name = 'odids_%d' % mission.number
                array = []
                for key, value in mission.odIDs_dictionary.iteritems():
                    array.append((key, value))
                array.sort()
                odids = []
                for key, value in array:
                    odids.append('%s: %s-%s' % (dayString(key), value[0], value[1]))
                row.add_to_cell(2, markup.input('text', name, ', '.join(odids), size=75))
            self.response.out.write(document.write())

    def overviewTable(self, missions):
        table = markup.HTMLTable('overview', ['Trein', 'Datum', 'Offset', 'Route', 'dV', 'Status'])
        for mission in missions:
            row = table.add_row()
            row.add_link_to_cell(0, '/console/schedule?mission=%s' % mission.id, mission.number)
            row.add_to_cell(1, mission.date_string)
            row.add_to_cell(2, mission.offset_string)
            if mission.stops:
                route = '%s - %s' % (mission.first_stop.station_id, mission.last_stop.station_id)
            else:
                route = '-'
            row.add_to_cell(3, route)

            (status, delay) = mission.status_at_time()
            row.add_to_cell(4, delay)
            row.add_to_cell(5, MissionStatuses.s[status])
        return table

    @property
    def series(self):
        if self._series is None:
            series_id = self.request.get('series')
            if series_id:
                self._series = TASeries.get(series_id)
        return self._series


def dayString(key):
    if key == '0':
        return 'ma'
    elif key == '1':
        return 'di'
    elif key == '2':
        return 'wo'
    elif key == '3':
        return 'do'
    elif key == '4':
        return 'vr'
    elif key == '5':
        return 'za'
    elif key == '6':
        return 'zo'
    elif key == 'd':
        return 'normaal'

class ScheduleHandler(webapp2.RequestHandler):

    def get(self):
        mission_id = self.request.get('mission')
        series_id = self.request.get('series')

        if mission_id:
            the_mission = TAMission.get(mission_id)

            if the_mission.up:
                label = 'heen'
            else:
                label = 'terug'
            document = ConsoleDocument('Trein %d (%s)' % (the_mission.number, label))
            the_series = the_mission.series
            if the_series:
                label = '%d treinen in serie %s' % (the_series.nr_of_missions, the_series.name)
                document.add_reference('/console/missions?kind=active&series=%s' % the_series.id, label)
            table = document.add_table('stops_table', ['s', 'A', 'V', 'dV', 'station', 'perron'])
            for the_stop in the_mission.stops:
                row = table.add_row()
                row.add_to_cell(0, the_stop.status)
                row.add_to_cell(1, the_stop.arrival_string)
                row.add_to_cell(2, the_stop.departure_string)
                delay = the_stop.delay_dep
                if delay > 0:
                    row.add_to_cell(3, '+%d' % delay)
                else:
                    row.add_to_cell(3, '-')
                row.add_link_to_cell(4, '/console/departures?station=%s' % the_stop.station.code, the_stop.station.name)
                if the_stop.platformChange:
                    row.add_to_cell(5, '%s <<' % the_stop.platform)
                else:
                    row.add_to_cell(5, the_stop.platform)
            self.response.out.write(document.write())

        elif series_id:
            the_series = TASeries.get(series_id)
            up = self.request.get('direction') != 'down'
            if up:
                label = 'heen'
                reverse = 'down'
                reverse_label = 'toon terugrichting'
                points_list = the_series.points
            else:
                label = 'terug'
                reverse = 'up'
                reverse_label = 'toon heenrichting'
                points_list = reversed(the_series.points)

            document = ConsoleDocument('Treinserie %s (%s)' % (the_series.name, label))
            document.add_reference('/console/schedule?series=%s&direction=%s' % (series_id, reverse),reverse_label)
            table = document.add_table('stops_table', ['km', 'station', 'aankomst', 'vertrek', 'perron', 'Jan', 'Feb'])
            for point in points_list:
                row = table.add_row()
                row.add_to_cell(0, '%.1f' % point.km)
                row.add_to_cell(1, point.stationName)
                if up:
                    row.add_to_cell(2, str(point.upArrival))
                    row.add_to_cell(3, str(point.upDeparture))
                    row.add_to_cell(4, point.platform_string(Direction.up))
                    avarage, delay = point.delayStats(Direction.up, 1)
                    if avarage != None:
                        row.add_to_cell(5, '%.1f - %.1f' % (avarage, delay))
                    avarage, delay = point.delayStats(Direction.up, 2)
                    if avarage != None:
                        row.add_to_cell(6, '%.1f - %.1f' % (avarage, delay))
                else:
                    row.add_to_cell(2, str(point.downArrival))
                    row.add_to_cell(3, str(point.downDeparture))
                    row.add_to_cell(4, point.platform_string(Direction.down))
                    avarage, delay = point.delayStats(Direction.down, 1)
                    if avarage != None:
                        row.add_to_cell(5, '%.1f - %.1f' % (avarage, delay))
                    avarage, delay = point.delayStats(Direction.down, 2)
                    if avarage != None:
                        row.add_to_cell(6, '%.1f - %.1f' % (avarage, delay))
            self.response.out.write(document.write())


# HTML Document

class ConsoleDocument(markup.HTMLDocument):

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

    def add_table(self, name, columnTitles):
        table = markup.HTMLTable(name, columnTitles)
        self.main.add(table)
        return table

    def add_page_navigator(self, currentPage, lastPage, urlFormat):
        self.main.add(markup.page_navigator(currentPage, lastPage, urlFormat))


# Helper function

def delay_string(delay):
    if delay > 0:
        return '+%d' % delay
    else:
        return '-'


def string_from_minutes(minutes):
    return '%d:%02d' % (minutes // 60, minutes % 60)


# WSGI Application

app = webapp2.WSGIApplication([('/console/?', ConsoleHandler),
                               ('/console/stations.*', StationsHandler),
                               ('/console/departures.*', DeparturesHandler),
                               ('/console/series.*', SeriesHandler),
                               ('/console/missions.*', MissionsHandler),
                               ('/console/schedule.*', ScheduleHandler)
                              ], debug=True)
