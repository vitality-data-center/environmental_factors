#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_landuse_p4
# Purpose:  Calculate the landuse diversity for each pc4 area in The Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import psycopg2
import math
import datetime
import logging




linux_mode = True
batch_num = 100
landuse_class_list = []
landuse_residential_tuple=(20, )
landuse_recreational_tuple=(40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83)
landuse_other_tuple=(10, 11, 12, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35)
landuse_class_list.append(landuse_residential_tuple)
landuse_class_list.append(landuse_recreational_tuple)
landuse_class_list.append(landuse_other_tuple)




logger = logging.getLogger()
logger.setLevel(logging.INFO)   
 
if linux_mode: 
    logfile = '/home/neil/log/logger.txt'
else:
    logfile = 'J://vdc_workspace//vdc//log//logger.txt'

fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)   
 
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)



def calc_landuse_index(pc4, pc4_area):
    
    global landuse_class_list
    landuse_class_num = len(landuse_class_list)
    
    landuse_class_idx = 0.0
    prop_sum = 0.0
    land_prop_list = []
    
    for i in range(len(landuse_class_list)):
        landuse_classes = landuse_class_list[i]
        prop = calc_landuse_prop(pc4, pc4_area, landuse_classes)
        prop_sum = prop_sum +prop
        land_prop_turple = (i, prop)
        land_prop_list.append(land_prop_turple)
        if prop > 0:
            landuse_class_idx = landuse_class_idx + prop * math.log(prop) / math.log(landuse_class_num)

    landuse_class_idx = - landuse_class_idx
    return landuse_class_idx, land_prop_list, prop_sum


def update_multiple_landuse_idx(records):
    
    try:
        cur_update, conn_update = connect()
        cur_update.executemany("UPDATE esri_pc4_2015r1 SET landuse_idx = %s where pc4 = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()






def calc_landuse_prop(pc4, pc4_area, landuse_classes):
    cur_1, conn_1 = connect()
    cur_1.execute("select b.id, b.bg2015, st_area(ST_Intersection(a.geom, b.geom)) from esri_pc4_2015r1 a, bbg2015 b where a.pc4 =  %s and b.bg2015 in %s and ST_Intersects(a.geom, b.geom);", ([pc4, landuse_classes]))

    result_set_1 = cur_1.fetchall()
    if  cur_1.rowcount==0:
        # print 'postcode = ', pc4, ' is not overlapped with any land use', landuse_classes
        return 0.0
    
    landuse_area_sum  = 0.0
    for record in result_set_1:
        landuse_id  = record[0]
        landuse_type  = record[1]
        landuse_area = record[2]
        # print "pc4 = ", pc4,  "landuse_id = ", landuse_id, " ; landuse_type = ", landuse_type, " landuse_area = ", landuse_area, " ; pc4_area = ", pc4_area, " ; prop = ", landuse_area / pc4_area
        landuse_area_sum = landuse_area_sum + landuse_area


    cur_1.close()
    conn_1.close()  
    
    landuse_prop = landuse_area_sum / pc4_area     
                      
    return  landuse_prop




def connect():
    
    conn = None    
    if linux_mode:
        conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
    else:
        conn = psycopg2.connect(database="postgres_nl", user="postgres", password="postgres", port="5433")
    cur = conn.cursor()
    return cur, conn




cur, conn = connect()

# drop column if exist and then create route column 

cur.execute("""
  ALTER TABLE esri_pc4_2015r1 DROP COLUMN IF EXISTS landuse_idx;
  ALTER TABLE esri_pc4_2015r1 ADD COLUMN landuse_idx double precision;""")
conn.commit()


# get walk data hvm = 22; motivation: sport, leisure, touring 
cur.execute("select pc4, st_area(geom) from esri_pc4_2015r1;")
record_result_set = cur.fetchall()

record_number =  cur.rowcount

row_index = 0
landuse_index_list = []


starttime =datetime.datetime.now()
      
for row in record_result_set:
    row_index = row_index + 1
    pc4 = row[0]
    pc4_area = row[1]
    landuse_index_tuple=()
        
    landuse_idx,  land_prop_list, prop_sum = calc_landuse_index(pc4, float(pc4_area))
    
    if  abs(landuse_idx) < 0.000001:
        landuse_idx = 0.000001
        print "not normal landuse index for pc4 = ", int(pc4),  " landuse index = ", landuse_idx, " land_prop_list = ", land_prop_list, " prop_sum = ", prop_sum
        logging.info('"not notmal landuse index for pc4 = ' + str(int(pc4)) + " ; lanuse index number = " + str(landuse_idx) )
    
    landuse_index_tuple = (landuse_idx, pc4)
    landuse_index_list.append(landuse_index_tuple)
    
    if row_index % batch_num == 0:
        
        update_multiple_landuse_idx(landuse_index_list)
        endtime =datetime.datetime.now()
        print "for pc4 ", int(pc4),  row_index, "; running time = ", (endtime - starttime).seconds/60, " mins! ", (endtime - starttime).seconds, " seconds!"
        
        # empty list
        starttime =datetime.datetime.now()
        landuse_index_list = []
    
    #if row_index==20:
    #    break    


update_multiple_landuse_idx(landuse_index_list)
cur.close()
conn.close() 
    