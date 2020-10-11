#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_air_p4
# Purpose:  Calculate the average air pollution concentrations for each pc4 area in The Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import psycopg2
from itertools import product
import numpy as np
import string
import sys
import re
import math
from decimal import Decimal 
import datetime
import logging



logger = logging.getLogger()
logger.setLevel(logging.INFO)   
 
logfile = '/home/neil/log/logger.txt'
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)   
 
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)



def connect():
    conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
    cur = conn.cursor()
    return cur, conn


#ST_Intersection(ST_MakeValid(b.geom), ST_Transform(a.geom_wgs84, 28992)) 



def update_all_air_idx(records):
    
    try:
        cur_update, conn_update = connect()
        cur_update.executemany("UPDATE esri_pc4_2015r1 SET no2_avg = %s, nox_avg = %s, pm25_avg = %s, pm10_avg = %s where pc4 = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    

def calc_air_idx(pc4, pc4_area):
    cur_air, conn_air = connect()
    cur_air.execute("select b.no2_avg, b.pm25_avg, b.pm10_avg, ST_Area(ST_Intersection(b.geom, a.geom)) from esri_pc4_2015r1 as a, air_pollution_nl_2015 as b where ST_Intersects(b.geom, a.geom) and a.pc4 = %s;", ([pc4]))
    
    result_set_air = cur_air.fetchall()
    if  cur_air.rowcount==0:
        print ('postcode = ', pc4), ' is not overlapped with any air pollution zones'
        return 0, 0, 0, 0
    
    no2_avg  = 0.0
    pm25_avg = 0.0
    pm10_avg = 0.0
    rate_sum = 0.0
    
    for record in result_set_air:
        #print "air index record = ", record
        no2_area  = record[0]
        pm25_area = record[1]
        pm10_area = record[2]
        intsec_area = record[3]
        
        rate = intsec_area/pc4_area
        no2_avg = no2_avg + no2_area * rate
        pm25_avg = pm25_avg + pm25_area * rate
        pm10_avg = pm10_avg + pm10_area * rate
        rate_sum = rate_sum + rate

    cur_air.close()
    conn_air.close()          
                      
    return no2_avg, pm25_avg, pm10_avg, rate_sum

  
def calc_nox_idx(pc4, pc4_area):
    cur_nox, conn_nox = connect()
    cur_nox.execute("select b.nox_avg, ST_Area(ST_Intersection(b.geom, a.geom)) from esri_pc4_2015r1 as a, nox_nl_2km_2015 as b where ST_Intersects(b.geom, a.geom) and a.pc4 = %s;", ([pc4]))
    
    result_set_nox = cur_nox.fetchall()
    if  cur_nox.rowcount==0:
        print ('postcode = ', pc4), ' is not overlapped with any nox zones'
        return 0, 0
    
    nox_avg  = 0.0
    rate_sum = 0.0
    
    for record in result_set_nox:
        # print "nox record = ", record
        nox_area  = record[0]
        intsec_area = record[1]
        
        rate = intsec_area/pc4_area
        nox_avg = nox_avg + nox_area * rate
        rate_sum = rate_sum + rate

    cur_nox.close()
    conn_nox.close()          
                      
    return nox_avg, rate_sum




cur, conn = connect()

# drop column if exist and then create route column 

cur.execute("""
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS no2_avg;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN no2_avg double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS nox_avg;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN nox_avg double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS pm25_avg;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN pm25_avg double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS pm10_avg;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN pm10_avg double precision;""")
conn.commit()


# get walk data hvm = 22; motivation: sport, leisure, touring 
cur.execute("select pc4, shape_area, ST_AsText(geom_wgs84) pc from esri_pc4_2015r1;")
record_result_set = cur.fetchall()

record_number =  cur.rowcount

row_index = 0
air_index_list = []
starttime =datetime.datetime.now()
      
for row in record_result_set:
    row_index = row_index + 1
    noise_index_tuple=()
    pc4 = row[0]
    pc4_area = row[1]
    geom_wgs84 = row[2]
    no2_avg, pm25_avg, pm10_avg, rate_sum1 = calc_air_idx(pc4, float(pc4_area))
    nox_avg, rate_sum2 = calc_nox_idx(pc4, float(pc4_area))
    
    if  abs(rate_sum1 - 1) > 0.0001 or abs(rate_sum2 - 1) > 0.0001:
        print "something wrong with calc air index for pc4 = ", pc4,  rate_sum1, rate_sum2
        print "pc4 = ", pc4, "; area = ", pc4_area , "; no2_avg = ", no2_avg, "; nox_avg = ", nox_avg, "; pm25_avg = ", pm25_avg , "; pm10_avg = ", pm10_avg
        logging.info('"something wrong with calc air index for pc4 = ' + str(pc4) + " ; sum1 = " + str(rate_sum1) + " ; sum2 = " + str(rate_sum2))
        logging.info("pc4 = " + str(pc4) + "; area = " +  str(pc4_area) + "; no2_avg = " +  str(no2_avg) + "; nox_avg = " + str(nox_avg) + "; pm25_avg = " + str(pm25_avg) + "; pm10_avg = " + str(pm10_avg))
    
    air_index_tuple = (no2_avg, nox_avg, pm25_avg, pm10_avg, pc4)
    air_index_list.append(air_index_tuple)
    
    if row_index % 10 == 0:
        
        update_all_air_idx(air_index_list)
        endtime =datetime.datetime.now()
        print "for pc4 ", pc4,  row_index, "; running time = ", (endtime - starttime).seconds/60, " mins! ", (endtime - starttime).seconds, " seconds!"
        
        # empty list
        starttime =datetime.datetime.now()
        noise_index_list = []
    
    #if row_index==20:
    #    break    


update_all_air_idx(air_index_list)
cur.close()
conn.close() 
    

    
    
    