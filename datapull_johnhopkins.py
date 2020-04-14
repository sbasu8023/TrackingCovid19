# -*- coding: utf-8 -*-
"""
Spyder Editor

Author: Sanjoy Basu
Date: 2020-03-28
"""
import requests
import datetime
import csv
import time
import os
import json
import geohash2
import boto3

#! /root/anaconda3/bin/python
# Global variables

import requests
import datetime
import csv
import time
import os
import json
import geohash2
import boto3

#! /root/anaconda3/bin/python
# Global variables

url_path='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
temp_dir='/tmp/'
schema=['FIPS','Admin2','State','Country','Last_Update','Lat','Lon','Confirmed','Deaths','Recovered','Active','Combined_Key']
schematype=['int','str' ,'str','str','str', 'float', 'float', 'int', 'int', 'int','int', 'str']
schema_final=['FIPS','Admin2','State','Country','Last_Update','Location','Location_hash', 'Confirmed','Deaths','Recovered','Active','Combined_Key']
delivery_stream="ncovid19Trk"

### Functions
def get_type(tp, val):
    if val:
        if tp=='int':
            return int(val)
        elif tp=='float':
            return float(val)
        else:
            return val
    else:
        return val

def get_filename(today):
    # Generate file name (string) to be pulled 
    dlimit='-'
    return dlimit.join([today.strftime("%m"), today.strftime("%d"), today.strftime("%Y")])
    
def get_dataURL(datafile):
    # Construct url to pull today's date import datetime.datetime
    return url_path+datafile+'.csv'

def get_tmppath(datafile):
    # Generate temporary path (string)
    return temp_dir+datafile+'.csv'

def get_localpath(datafile):
    # Generate log path (string)
    return local_dir+datafile

def fix_time(dt): 
          if '/' in dt: 
              delim='-' 
              spl=dt.split('/') 
              if len(spl[0])==1: 
                  month='0'+spl[0] 
              else: 
                  month=spl[0] 
              if len(spl[1])==1: 
                  day='0'+spl[1] 
              else: 
                  day=spl[1] 
              time=spl[2][3:8]+":00" 
              newdt=delim.join(['2020',month,day]) 
              return newdt+" "+time 
          else: 
              return dt
          
            
            
def get_data():
    # Pulling data from external source
    delta=datetime.timedelta(days=1)
    daytopull=datetime.datetime.now()-delta
    tmppath=get_tmppath(get_filename(daytopull))
    uri=get_dataURL(get_filename(daytopull))
    try:
        rq = requests.get(uri)
    except requests.exceptions.RequestException as e:  
        raise SystemExit(e)
    with open(tmppath, 'wb') as fl:
        fl.write(rq.content)
    
    return tmppath

def put2_stream():
    # Removing headers before sending to stream
    # CSV to be converted to JSON before putting in the stream
    src_path=get_data()
    client = boto3.client("firehose", region_name="us-east-1")
    with open(src_path, 'r') as source:
        reader = csv.reader(source)
            
        next(reader) # removing headers
        for row in reader:
            if row[3]=="US" and row[2]!='Diamond' and row[5]:
                row[4]=fix_time(row[4]) 
                values=[get_type(schematype[i], k) for i, k in enumerate(row)  if i <5 or i >6]
                location={"lat":get_type('float',row[5]) ,"lon": get_type('float',row[6])}
                loc_hash=geohash2.encode(get_type('float',row[5]), get_type('float',row[6]))
                values.insert(5, location); values.insert(6, loc_hash)
                time.sleep(0.2) # Slowing down streaming
                response=client.put_record(DeliveryStreamName=delivery_stream,Record={'Data': json.dumps(dict(zip(schema_final,values)))})
                #print(json.dumps(dict(zip(schema_final,values))))
    os.remove(src_path)

    print(response)
def main():
    put2_stream()

if __name__ == "__main__":
    main()        



