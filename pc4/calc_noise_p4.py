#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_noise_p4
# Purpose:  Calculate the average noise level for each pc4 area in The Netherlands
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

def connect():
    conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
    cur = conn.cursor()
    return cur, conn


#ST_Intersection(ST_MakeValid(b.geom), ST_Transform(a.geom_wgs84, 28992)) 



def update_all_noise(records):
    
    try:
        cur_update, conn_update = connect()
        cur_update.executemany("UPDATE esri_pc4_2015r1 SET dn_1 = %s, dn_2 = %s, dn_3 = %s, dn_4 = %s, dn_5 = %s, dn_6 = %s where pc4 = %s",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    


def update_one_noise(pc4, dn_1, dn_2, dn_3, dn_4, dn_5, dn_6):
    
    cur_update, conn_update = connect()
    cur_update.execute("UPDATE esri_pc4_2015r1 SET dn_1 = %s, dn_2 = %s, dn_3 = %s, dn_4 = %s, dn_5=%s, dn_6= %s where pc4 = %s", ([dn_1, dn_2, dn_3, dn_4, dn_5, dn_6, pc4]))
    conn_update.commit()
    cur_update.close()
    conn_update.close()



def calc_noise_distributions(pc4, pc4_area):
    cur_noise, conn_noise = connect()
    cur_noise.execute("select b.dn, sum(ST_Area(ST_Intersection(b.geom, a.geom))) from esri_pc4_2015r1 as a, noise_map_nl_2016 as b where ST_Intersects(b.geom, a.geom) and a.pc4 = %s  GROUP BY b.dn;", ([pc4]))
    
    result_set_noise = cur_noise.fetchall()
    if  cur_noise.rowcount==0:
        print ('postcode = ', pc4), ' is not overlapped with any noise zones'
        return -2, -2
    
    dn_1 = 0.0 
    dn_2 = 0.0 
    dn_3 = 0.0     
    dn_4 = 0.0 
    dn_5 = 0.0 
    dn_6 = 0.0 
    
    for record in result_set_noise:
        #print "noise record = ", record
        noise_dn = record[0]
        noise_area = record[1]
        if noise_dn == 1:
            dn_1 = noise_area/pc4_area
        elif noise_dn == 2:
            dn_2 = noise_area/pc4_area 
        elif noise_dn == 3:
            dn_3 = noise_area/pc4_area     
        elif noise_dn == 4:
            dn_4 = noise_area/pc4_area     
        elif noise_dn == 5:
            dn_5 = noise_area/pc4_area                     
        else:
            dn_6 =  dn_6 + noise_area/pc4_area     
                      
    cur_noise.close()
    conn_noise.close()   
    
    
    return dn_1, dn_2, dn_3, dn_4, dn_5, dn_6








def calc_noise_intersections(pc4):
    cur_noise, conn_noise = connect()
    cur_noise.execute("select b.dn, sum(ST_Area(ST_Intersection(b.geom, ST_Transform(a.geom_wgs84, 28992)))) from esri_pc4_2015r1 as a, noise_map_nl_2016 as b where ST_Intersects(b.geom, ST_Transform(a.geom_wgs84, 28992)) and a.pc4 = %s  GROUP BY b.dn;", ([pc4]))
    
    result_set_noise = cur_noise.fetchall()
    if  cur_noise.rowcount==0:
        print ('postcode = ', pc4), ' is not overlapped with any noise zones'
        return -2, -2
    
    noise_max_area = -1
    noise_max_dn = -1 
    
    for record in result_set_noise:
        #print "noise record = ", record
        noise_dn = record[0]
        noise_area = record[1]
        
        if noise_area >= noise_max_area:
            noise_max_area = noise_area
            noise_max_dn = noise_dn 
                      
    return noise_max_area, noise_max_dn

    cur_noise.close()
    conn_noise.close()    




cur, conn = connect()

# drop column if exist and then create route column 

cur.execute("""
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_1;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_1 double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_2;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_2 double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_3;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_3 double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_4;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_4 double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_5;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_5 double precision;
ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS dn_6;
ALTER TABLE esri_pc4_2015r1 ADD COLUMN dn_6 double precision;""")
conn.commit()


# get walk data hvm = 22; motivation: sport, leisure, touring 
cur.execute("select pc4, shape_area, ST_AsText(geom_wgs84) pc from esri_pc4_2015r1;")
record_result_set = cur.fetchall()

record_number =  cur.rowcount

row_index = 0
noise_index_list = []
starttime =datetime.datetime.now()
      
for row in record_result_set:
    row_index = row_index + 1
    noise_index_tuple=()
    pc4 = row[0]
    pc4_area = row[1]
    geom_wgs84 = row[2]
    # noise_max_area, max_dn =  calc_noise_intersections(pc4)
    dn_1, dn_2, dn_3, dn_4, dn_5, dn_6 = calc_noise_distributions(pc4, float(pc4_area))
    #print "pc4 = ", pc4, "; area = ", pc4_area , "; geom_wgs84 = ", geom_wgs84, "; noise_max_area = ", noise_max_area, "; max_dn = ", max_dn
    dn_sum = dn_1 + dn_2 +  dn_3 + dn_4 + dn_5 + dn_6 
    if  abs(dn_sum - 1) > 0.0001:
        print "something wrong with calc area for pc4 ", pc4, "; dn_sum = ", dn_sum
    
    noise_index_tuple = (dn_1, dn_2, dn_3, dn_4, dn_5, dn_6, pc4)
    noise_index_list.append(noise_index_tuple)
    
    if row_index % 10 == 0:
        
        update_all_noise(noise_index_list)
        endtime =datetime.datetime.now()
        print "for pc4 ", pc4,  row_index, "; running time = ", (endtime - starttime).seconds/60, " mins! ", (endtime - starttime).seconds, " seconds!"
        
        # empty list
        starttime =datetime.datetime.now()
        noise_index_list = []
    
    #if row_index==10:
    #    break    


update_all_noise(noise_index_list)
cur.close()
conn.close() 
    

    
    
    