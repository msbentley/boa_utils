#!/usr/bin/python
"""
boa_utils.py

Mark S. Bentley (mark@lunartech.org), 2019

A module to use the BOA TAP API and retrieve/plot data.

Basic authentication credentials should be stored in a simple
YAML file and pointed at by the config_file parameter when
instantiated the Must class. An example is:

user:
    login: userone
    password: blah

"""

import yaml
import requests
from requests.auth import HTTPBasicAuth
import astropy
from astropy.io.votable import parse_single_table
from io import StringIO, BytesIO
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import warnings
import re

import logging
log = logging.getLogger(__name__)
warnings.simplefilter('ignore', category=astropy.io.votable.exceptions.VOTableSpecWarning)

default_url = 'https://boa.esac.esa.int/boa-tap/tap'
default_dl_url = 'https://boa.esac.esa.int/boa-sl'
default_config = default_config = os.path.expanduser('~/Dropbox/work/bepi/boa_tap.yml')

# expected timestamp format: 2019-04-27 07:49:11.688
date_format = '%Y-%m-%d %H:%M:%S.%f'

class BOA_tap:

    def __init__(self, url=default_url, dl_url=default_dl_url, config_file=default_config):
        """The BOA URL instance can be specified, along with a
        YAML config file containing a user section with login and
        password entries. If neither of these are provided, the
        default values are used"""

        self.url = url
        self.dl_url = dl_url
        self.config = None
        self.load_config(config_file)


    def _url(self, path, dl=False):
        """Helper function to append the path to the base URL"""
        
        if dl:
            return  self.dl_url + path
        else:
            return self.url + path


    def load_config(self, config_file):
        """Load the configuration file containing, at minimum,
        the username and password for BOA authentication"""

        try:
            f = open(config_file, 'r')
            self.config = yaml.load(f, Loader=yaml.BaseLoader)
        except FileNotFoundError:
            log.error('config file {:s} not found'.format(config_file))

        return


    def retrieve_data(self, query, binary=True, dl_path='.', extract=True):

        # the BOA retrieval syntax is poor and doesn't take parameters by default
        # but just mashes the query into the URL - so we have to deal with this,
        # but if telemetry packet is requested, and binary is True then we have
        # to build a proper request with the query encoded in the query parametrer

        if 'boa.telemetry_packet' not in query:
            query = requests.utils.quote(query)
            try:
                r = requests.get(url=self._url('/retrieve-data?'+query, dl=True), 
                    auth=HTTPBasicAuth(self.config['user']['login'], self.config['user']['password']))
                r.raise_for_status()
            except (requests.exceptions.RequestException, ConnectionResetError) as err:
                log.error(err)
                return None
            except (requests.exceptions.HTTPError) as err:
                log.error(err)
                return None

        else:
            default_payload = {
                'dataformat': 'GDDS' if binary else 'XML'}
            query = {'QUERY': query}
            params = default_payload.copy()
            params.update(query)

            try:
                r = requests.get(url=self._url('/retrieve-data', dl=True), params=params, 
                    auth=HTTPBasicAuth(self.config['user']['login'], self.config['user']['password']))
                r.raise_for_status()
            except (requests.exceptions.RequestException, ConnectionResetError) as err:
                log.error(err)

        filename = get_filename_from_cd(r.headers.get('content-disposition'))
        if filename is None:
            log.error('error retrieving filename from server response')
            return None
        dl_file = os.path.join(dl_path, filename)
        f = open(dl_file, 'wb')
        for chunk in r.iter_content(100000):
            f.write(chunk)
        f.close()
        
        log.info('retrieved file {:s}'.format(filename))

        if extract:
            import tarfile
            tar = tarfile.open(dl_file)
            tar.extractall(path=dl_path)
            filename = tar.getnames()
            filename = [os.path.join(dl_path, f) for f in filename]
            log.info('extracted file(s): {:s}'.format(', '.join(filename)))
            if len(filename)==1:
                filename = filename[0]

            return filename



    def make_query(self, query, maxrows=5000):

        default_payload = {
            'LANG': 'ADQL',
            'REQUEST': 'doQuery',
            'MAXREC': '{:d}'.format(maxrows)}
        query = {'QUERY': query}

        log.debug('Query: {:s}'.format(query['QUERY']))
        params = default_payload.copy()
        params.update(query)
        try:
            r = requests.get(url=self._url('/sync'), params=params, 
                auth=HTTPBasicAuth(self.config['user']['login'], self.config['user']['password']))
            r.raise_for_status()
        except (requests.exceptions.RequestException, ConnectionResetError) as err:
                log.error(err)

        # convert from a votable to an astropy table with to_table()
        table = parse_single_table(BytesIO(r.content), pedantic=False).to_table()
        cols = table.colnames

        # convert to a pandas DataFrame. cannot use .to_pandas() directly
        # since the colums have shape (1,) and pandas cannot handle
        # "multidimensional" columns.
        result = pd.DataFrame([], columns=cols)
        for col in cols:
            result[col] = table[col].data.squeeze()
        # TODO fix this for single value tables!

        # 2021-01-11 - removing this code for now since something has changed in
        # recently library updates - pandas? astropy? and it is no longer neededc
        #'
        # do the unicode conversion for string data types
        # str_df = result.select_dtypes([np.object])
        # if len(str_df)>0:
        #     str_df = str_df.stack().str.decode('utf-8').unstack()        
        #     for col in str_df:
        #         result[col] = str_df[col]

        return result


    def query_packets(self, start_time=None, stop_time=None, subsys=None, 
        spid=None, apid=None, pkt_type=None, pkt_subtype=None, maxrows=5000,
        reduced=True):

        if subsys is not None:
            subsystems = self.make_query('select distinct subsystem_id from subsystem').subsystem_id.tolist()
            if subsys not in subsystems:
                log.error('subsystem {:s} is not valid. Should be one of: {:s}'.format(str(subsys), ', '.join(subsystems)))
                return None
        else:
            subsys  = ''

        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=1)
        elif type(start_time) == str:
            start_time = pd.Timestamp(start_time)

        if stop_time is None:
            stop_time = pd.Timestamp.now()
        elif type(stop_time) == str:
            stop_time = pd.Timestamp(stop_time)

        query = "SELECT * FROM TELEMETRY_PACKET WHERE on_board_time >= '{:s}' and on_board_time <= '{:s}' and subsystem_id='{:s}'".format(
                start_time.strftime(date_format), stop_time.strftime(date_format), subsys)

        if pkt_type is not None:
            query += ' and source_packet_service_type={:d}'.format(pkt_type)

        if pkt_subtype is not None:
            query += ' and source_packet_service_subtype={:d}'.format(pkt_subtype)

        if spid is not None:
            query += ' and telemetry_packet_spid={:d}'.format(spid)

#        if order:
#            query += ' ORDER BY item_id'

        packets = self.make_query(query=query, maxrows=maxrows)

        drop_list = ['item_id', 'ground_station_id', 'mib_version', 'inactive',
            'ingested_time', 'bscs_ingestion_time', 'proprietary_end_date', 'retrieval_url',
            'telemetry_packet_oid']

        if reduced:
            packets.drop(drop_list, axis=1, inplace=True)

        time_cols = [col for col in packets.columns if 'time' in col.lower()]
        for col in time_cols:
            packets[col] = pd.to_datetime(packets[col])

        log.info('{:d} matching telemetry packets found'.format(len(packets)))

        if len(packets)==maxrows:
            log.warn('number of packets returned is limited by query - increase max_rows to see more')

        return packets
    


def get_events(instr=None, start_time=None, stop_time=None, get_descrip=False):

    if get_descrip:

        try:
            import bepicolombo
        except ModuleNotFoundError:
            log.warn('bepicolombo module not available, cannot display even descriptions')
            get_descrip=False

    boa = BOA_tap() 
    subsys = boa.make_query('select distinct subsystem_id from subsystem')
    if instr not in subsys.subsystem_id.tolist():
        log.error('subsystem {:s} is not valid'.format(str(instr)))
        return None
    
    events = boa.query_packets(subsys=instr.upper(), pkt_type=5, start_time=start_time, stop_time=stop_time)
    
    if get_descrip:
        # add description from the PID table in the MIB
        events = pd.merge(events, bepicolombo.bepi_tm.pid[['description', 'spid']], left_on='telemetry_packet_spid', right_on='spid')

    return events


def get_packets(instr=None, start_time=None, stop_time=None, dl_path='.', binary=True, extract=True):

    boa = BOA_tap() 
    subsys = boa.make_query('select distinct subsystem_id from subsystem')
    if instr not in subsys.subsystem_id.tolist():
        log.error('subsystem {:s} is not valid'.format(str(instr)))
        return None

    if start_time is None:
        start_time = pd.Timestamp.now() - pd.Timedelta(days=1)
    elif type(start_time) == str:
        start_time = pd.Timestamp(start_time)

    if stop_time is None:
        stop_time = pd.Timestamp.now()
    elif type(stop_time) == str:
        stop_time = pd.Timestamp(stop_time)

    query = "SELECT * FROM TELEMETRY_PACKET WHERE on_board_time >= '{:s}' and on_board_time <= '{:s}' and subsystem_id='{:s}'".format(
            start_time.strftime(date_format), stop_time.strftime(date_format), instr.upper())

    filename = boa.retrieve_data(query, dl_path=dl_path, binary=binary, extract=extract)

    return filename
    

def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None

    return fname[0].strip('\"')