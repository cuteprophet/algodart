#!/usr/bin/env python

import numpy as np
import csv, os
from datetime import datetime
#import matplotlib
import talib
#import dask.dataframe as df
import pandas as df
import dask.array as darr


CSV = 1
LIVE = 2
class Datafeed:
    '''
    The security datafeed
    '''
    data = None
    '''
    initial structure
    {
            'datetime':iso timestamp,
            'open':float64,
            'high':float64,
            'low':float64,
            'close':float64,
            'volume':int,
            'adjClose':float64
            }
    '''
    type = None
    source = None
    
    def __init__(self, source=None,type=None, **kwargs):
        '''
        initiallization
        source: 
            The source of the data feed, can be a file, webpage, url, or API for live feeding
        type:
            The type of the feed
        '''
        
        self.type = type
        self.source = source
        
        if self.type == CSV:
            '''
            if 'delimiter' in kwargs: delm = kwargs['delimiter'] or None
            if 'quotechar' in kwargs: qc = kwargs['quotechar'] or None
            '''
            
            if self.source is not None:
                self.loadCSV(self.source, **kwargs)
        
        
    def loadCSV(self, filename, delimiter=',', skiprows=None, dateformat="%Y.%m.%d", timeformat="%H:%M", 
                dateCol=0, timeCol=1, openCol=2, highCol=3, lowCol=4, closeCol=5, volumeCol=6, adjCloseCol=None, 
                **kwargs):
        '''
        Load the data from a csv file.  
        load the price in the order datetime, open, high, low, close, volume, adjClose, if not specified
        Or this order can be specified via the parameters. The orders of date and adjClose can be skipped
        filename:
            The csv filename
        delimiter:
            The seprator of CSV, default to be comma
        skiprows:
            Line numbers to skip (0-indexed) or number of lines to skip (int) at the start of the file.
        dateformat:
            The format to parse the date string
        timeformat:
            The format to parse the time string
        date, time, open, high, low, close, volume, adjClose:
            The order of these values in the loaded csv file, 0 based
        **kwargs:
            Refer to Dask dataframe read_csv **kwargs
        
        return
        '''
        assert os.path.exists(filename), f"The file {filepath} does not exist!"
        assert os.path.isfile(filename), f"The file {filepath} is not a file path."
        # Make sure the data feed is not overridden
        #assert self.source == None
        
        keys = []
        vals = []
        if dateCol is None or dateCol < 0:
            dateCol is None
            assert timeCol is not None and timeCol > 0, "time column order has to be set if data is not set"
        else:
            assert len(dateformat) > 0, "Date format has to be specified if date order is provided"
            keys.append(dateCol)
            vals.append('date')
            
        keys.append(timeCol)
        vals.append('datetime')
        
        keys.append(openCol)
        vals.append('open')
        
        keys.append(highCol)
        vals.append('high')
        
        keys.append(lowCol)
        vals.append('low')
        
        keys.append(closeCol)
        vals.append('close')
        
        keys.append(volumeCol)
        vals.append('volumn')
            
        #assert len(ll) == len(set(ll)), "Colume numbers for different variables are identical. "
        if adjCloseCol is None or adjCloseCol < 0:
            adjCloseCol = None
        else:
            keys.append(adjCloseCol)
            vals.append('adjClose')
            
        assert len(keys) == len(set(keys)), "The column index has to be identical"
        assert sorted(keys) == list(range(min(keys), max(keys)+1)), "The column index has to be consecutive"
        assert min(keys) == 0, "The column index has to start from 0"
        
        col = dict(zip(keys, vals))
        # Get the column names in sorted order
        colNames = [item[1] for item in sorted(col.items())]
        
        self.data = df.read_csv(self.source, header=None, skiprows=skiprows, names=colNames, **kwargs)
        
        toDropCols = []
        # Date and time are saved combined and in timestamp
        if 'date' in colNames:
            # if date is specified, date and time have to be combined
            self.data['tmp'] = self.data['date'] + ' ' + self.data['datetime']
            self.data['datetime'] = df.to_datetime(self.data['tmp'], format=' '.join([dateformat, timeformat]))
            toDropCols.append('date')
            toDropCols.append('tmp')
        else:
            self.data['datetime'] = df.to_datetime(self.data['datetime'] , format = timeformat)
            
        self.data.drop(toDropCols, axis=1)
            
        self.source = filename
        self.type = CSV
        
        return
            
    @property
    def datetime(self):
        return self.data['datetime']
    @property
    def open(self):
        return self.data['open']
    @property
    def high(self):
        return self.data['high']
    @property
    def low(self):
        return self.data['low']
    @property
    def close(self):
        return self.data['close']
    @property
    def volume(self):
        return self.data['volume']
    @property
    def adjClose(self):
        return self.data['adjClose']

class CSVDatafeed(Datafeed):
    '''
    A overload of Datafeed for CSV data
    '''
    def __init__(self, source=None, **kwargs):
        '''
        initiallization
        source: 
            The source of the data feed, can be a file, webpage, url, or API for live feeding
        type:
            The type of the feed
        '''
        
        self.type = CSV
        assert len(source) > 0, 'The CSV file path have to be passed'
        self.source = source
        '''
        if 'delimiter' in kwargs: delm = kwargs['delimiter'] or None
        if 'quotechar' in kwargs: qc = kwargs['quotechar'] or None
        '''   
        self.loadCSV(self.source, **kwargs)
    
class MT4CSVDatafeed(CSVDatafeed):
    '''
    A overload of Datafeed for MT4 history exported CSV data
    '''
    def __init__(self, source=None, **kwargs):
        '''
        initiallization
        source: 
            The source of the data feed, can be a file, webpage, url, or API for live feeding
        type:
            The type of the feed
        '''
        
        self.type = CSV
        assert len(source) > 0, 'The CSV file path have to be passed'
        self.source = source
        '''
        if 'delimiter' in kwargs: delm = kwargs['delimiter'] or None
        if 'quotechar' in kwargs: qc = kwargs['quotechar'] or None
        '''   
        self.loadCSV(self.source, delimiter=',', dateformat="%Y.%m.%d", timeformat="%H:%M", 
                     dateCol=0, timeCol=1, openCol=2, highCol=3, lowCol=4, closeCol=5, volumeCol=6, 
                     **kwargs)
        

    
    