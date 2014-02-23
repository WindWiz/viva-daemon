#! /usr/bin/env python2
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

"""ViVa Python API

This file implements a basic API to access the ViVa SOAP service.

Example usage:

    import viva

    # Retrieve list of available stations
    stations = viva.fetch_station_list()

    ..

    for station in stations:
        print("Station %s (%d)" % (station['id'], station['name']))

        # Get the latest measured samples
        latest_samples = viva.fetch_station_latest(station['id'])
        print(latest_samples)

        # Get samples for the last 24 hours
        to = datetime.today()
        from = to - timedelta(days=1)
        old_samples = viva.fetch_station_history(station['id'], from, to)
        print(old_samples)

Each sample contains a type (wind, temperature, water flow etc), its value and
the timestamp when it was measured.

NOTE: ViVa weather stations do not measure all types at the same time or at the
same frequency.

For this reason, it is common that you'll receive samples for different types
spread across a long timespan. For example, the latest gust speed may be
recorded at time X while the average wind speed is recorded time Y.
While the difference between X and Y is often only a few seconds, they sometimes
differ several minutes, sometimes even hours.

This is annoying because you can never really tell what the exact conditions
were at time X or Y, you only know bits and pieces.
"""

import urllib2
import pytz
from calendar import timegm
from datetime import datetime
from datetime import timedelta
from lxml import etree
import logging

# The user-agent passed to ViVa
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64)'

# ViVa SOAP service
VIVA_URL = "http://161.54.134.239/vivadata.asmx"
VIVA_DATEFORMAT = '%Y-%m-%dT%H:%M:%S'

NS = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'vivaresp': 'http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl',
    'vivares': 'http://www.sjofartsverket.se/scheman/vaderdata/ViVaOutputSchema.xsd',
    'vivap': 'http://www.sjofartsverket.se/scheman/vaderdata/ViVaPointsSchema.xsd'
}

VIVA_LIST_REQUEST = """
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <GetViVaPoints xmlns="http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl" />
    </s:Body>
</s:Envelope>
"""

VIVA_HISTORY_REQUEST = """
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <GetViVaDataTH xmlns="http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl">
            <PlatsId>%s</PlatsId>
            <ViVaTyp>%d</ViVaTyp>
            <TidFOM>%s</TidFOM>
            <TidTOM>%s</TidTOM>
        </GetViVaDataTH>
    </s:Body>
</s:Envelope>
"""

VIVA_STATION_REQUEST = """
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <GetViVaDataT xmlns="http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl">
            <PlatsId>%d</PlatsId>
        </GetViVaDataT>
    </s:Body>
</s:Envelope>
"""

XPATH_SAMPLE_HISTORY = 'soap:Body/vivaresp:GetViVaDataTHResponse/vivaresp:GetViVaDataTHResult/vivaresp:ViVaPoint'
XPATH_STATION_LIST = 'soap:Body/vivaresp:GetViVaPointsResponse/vivap:GetViVaPointsResult/vivap:ViVaPoint'
XPATH_LATEST_ERRORMSG = 'soap:Body/vivaresp:GetViVaDataTResponse/vivares:GetViVaDataTResult/vivares:Felmeddelande'
XPATH_LATEST_NAME = 'soap:Body/vivaresp:GetViVaDataTResponse/vivares:GetViVaDataTResult/vivares:PlatsNamn'
XPATH_LATEST_SAMPLES = 'soap:Body/vivaresp:GetViVaDataTResponse/vivares:GetViVaDataTResult/vivares:ViVaDataT'

# All ViVa timestamps are Central European Timezone (CET)
TZ_CET = pytz.timezone('CET')

SAMPLE_LUT = {
    (u'MEDELVIND',        u'm/s'):  'AVG_WIND',
    (u'BYVIND',           u'm/s'):  'GUST_WIND',
    (u'RIKTNING',         u'°'):    'WIND_DIRECTION',
    (u'VATTENTEMPERATUR', u'°'):    'WATER_TEMP',
    (u'SIKT',             u'm'):    'VISIBILITY',
    (u'LUFTTEMPERATUR',   u'°'):    'AIR_TEMP',
    (u'LUFTFUKTIGHET',    u'%'):    'AIR_HUMIDITY',
    (u'LUFTTRYCK',        u'mbar'): 'AIR_PRESSURE',
    (u'VATTENSTÅND',      u'cm'):   'WATER_LEVEL',
    (u'STRÖM 2M',         u'°'):    'WATER_CURRENT_2M_DIR',
    (u'STRÖM 2M',         u'knop'): 'WATER_CURRENT_2M_SPEED',
    (u'STRÖM 4M',         u'°'):    'WATER_CURRENT_4M_DIR',
    (u'STRÖM 4M',         u'knop'): 'WATER_CURRENT_4M_SPEED',
    (u'STRÖM 6M',         u'°'):    'WATER_CURRENT_6M_DIR',
    (u'STRÖM 6M',         u'knop'): 'WATER_CURRENT_6M_SPEED',
    (u'STRÖM YTA',        u'°'):    'WATER_CURRENT_SURFACE_DIR',
    (u'STRÖM YTA',        u'knop'): 'WATER_CURRENT_SURFACE_SPEED',
}

class Sample:
    def __init__(self, station_id, station_name, stype, svalue, ststamp):
        self.station_id = station_id
        self.station_name = station_name
        self.stype = stype
        self.svalue = svalue
        self.ststamp = ststamp

    def __str__(self):
        return '%s (%d) -- %s = %s at %s' % (self.station_name,
                                             self.station_id,
                                             self.stype,
                                             self.svalue,
                                             self.ststamp)

def create_sample(station_id, station_name, sunit, stype, svalue, ststamp):
    log = logging.getLogger('vivad.viva')

    if (stype, sunit) in SAMPLE_LUT:
        stype = SAMPLE_LUT[(stype, sunit)]
        sample = Sample(station_id, station_name, stype, svalue, ststamp)
        log.debug(sample)
        return sample
    else:
        log.debug(" - %s (unit %s) ignored (unknown type)" % (stype, sunit))
        return False

def fetch_station_list():
    ''' Retrieves the global station list/index from ViVa SOAP service '''

    req_headers = {
        "User-Agent" : USER_AGENT,
        "soapaction": '"http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl/GetViVaPoints"',
        "Content-Type": "text/xml; charset=utf-8"
    }

    data = VIVA_LIST_REQUEST
    log = logging.getLogger('vivad.viva')

    request = urllib2.Request(VIVA_URL, data=data, headers=req_headers)
    stream = urllib2.urlopen(request)

    tree = etree.parse(stream)
    log.debug(etree.tostring(tree, pretty_print=True))

    stations = []
    station_elements = tree.xpath(XPATH_STATION_LIST, namespaces=NS)
    for element in station_elements:
        stations.append({
            'id': element.get('PlatsId'),
            'name': element.get('Platsnamn'),
            'lat': element.get('Latitude'),
            'lon': element.get('Longitude')
        })

    return stations

def fetch_station_history(station_id, time_from, time_until, hist_type = 0):
    '''
    Fetch station sample history

    Retrieves historic samples of given type for the given station between the
    specified dates.

    hist_type determines the type of sample to retrieve.

    hist_type       sample type
    ---------       -----------
    0               All types (default)
    11              AVG_WIND
    12              GUST_WIND
    13              WIND_DIRECTION
    14              WATER_LEVEL
    15              AIR_TEMP
    16              WATER_TEMP

    There are additional hist_types, however these are not known/mapped yet.
    '''

    req_headers = {
        "User-Agent" : USER_AGENT,
        "soapaction": '"http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl/GetViVaDataTH"',
        "Content-Type": "text/xml; charset=utf-8"
    }

    data = VIVA_HISTORY_REQUEST
    log = logging.getLogger('vivad.viva')

    str_from = datetime.strftime(time_from, VIVA_DATEFORMAT)
    str_until = datetime.strftime(time_until, VIVA_DATEFORMAT)

    data = data % (station_id, hist_type, str_from, str_until)

    request = urllib2.Request(VIVA_URL, data=data, headers=req_headers)
    stream = urllib2.urlopen(request)
    tree = etree.parse(stream)
    log.debug(etree.tostring(tree, pretty_print=True))

    elements = tree.xpath(XPATH_SAMPLE_HISTORY, namespaces=NS)
    samples = []
    for element in elements:
        station_name = element.get('Namn')
        sample_type = element.get('TypNamn').upper()
        sample_unit = element.get('Enhet')
        sample_value = element.get('Data')
        sample_tstamp = datetime.strptime(element.get("Tid"), VIVA_DATEFORMAT)
        sample_tstamp = sample_tstamp.replace(tzinfo=TZ_CET)

        sample = create_sample(station_id, station_name, sample_unit,
                               sample_type, sample_value, sample_tstamp)
        if sample:
            samples.append(sample)

    log.info('Found %d samples of type %d for station %d between %s -> %s' % (len(samples), hist_type, station_id, str_from, str_until))
    return samples

def fetch_station_latest(station_id):
    ''' Fetch the latest recorded samples from ViVa for the given station id '''

    req_headers = {
        "User-Agent" : USER_AGENT,
        "soapaction": '"http://www.sjofartsverket.se/webservice/VaderService/ViVaData.wsdl/GetViVaDataT"',
        "Content-Type": "text/xml; charset=utf-8"
    }

    data = VIVA_STATION_REQUEST
    data = data % station_id
    log = logging.getLogger('vivad.viva')

    request = urllib2.Request(VIVA_URL, data=data, headers=req_headers)
    stream = urllib2.urlopen(request)
    tree = etree.parse(stream)
    log.debug(etree.tostring(tree, pretty_print=True))

    # Look for errors in the XML response
    err_element = tree.xpath(XPATH_LATEST_ERRORMSG, namespaces=NS)
    if err_element:
        log.error("error: An error was for station %d: %s" % (station_id, err_element[0].text))
        return False

    name_element = tree.xpath(XPATH_LATEST_NAME, namespaces=NS)
    if not name_element:
        log.error("error: No location name for station %d" % station_id)
        return False

    station_name = name_element[0].text
    elements = tree.xpath(XPATH_LATEST_SAMPLES, namespaces=NS)
    if not elements:
        log.error("error: No samples found for station '%s' (%d)" % (station_name, station_id))
        return False

    log.info('Retrieved %d samples for %s (%d)' % (len(elements), station_name, station_id))

    samples = []
    for element in elements:
        sample_type = element.get("Typ").upper()
        sample_value = element.get("Varde")
        sample_unit = element.get("Enhet")
        sample_tstamp = datetime.strptime(element.get("Tid"), VIVA_DATEFORMAT)
        sample_tstamp = sample_tstamp.replace(tzinfo=TZ_CET)

        sample = create_sample(station_id, station_name, sample_unit,
                               sample_type, sample_value, sample_tstamp)
        if sample:
            samples.append(sample)

    return samples
