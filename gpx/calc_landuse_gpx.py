#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_landuse_gpx
# Purpose:  Calculate the land use diversity around gpx tracks
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import psycopg2
import string
import sys
import re
import math
import utility_fun2 as uf
import logging
import time



logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table
buffer_area = 0.0; 
pixel_area = 0.0; 



# landuse_class_list = []
# landuse_residential_tuple=(20, )
# landuse_recreational_tuple=(40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83)
# landuse_other_tuple=(10, 11, 12, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35)
# landuse_class_list.append(landuse_residential_tuple)
# landuse_class_list.append(landuse_recreational_tuple)
# landuse_class_list.append(landuse_other_tuple)


landuse_class_list = []
landuse_residential_tuple=(20, )
landuse_recreational_tuple=(40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83)
landuse_other_tuple=(10, 11, 12, 22, 23, 30, 31, 32, 33, 34, 35)
landuse_commercial_tuple = (21,)
landuse_industrial_tuple = (24,)
landuse_class_list.append(landuse_residential_tuple)
landuse_class_list.append(landuse_recreational_tuple)
landuse_class_list.append(landuse_commercial_tuple)
landuse_class_list.append(landuse_industrial_tuple)
landuse_class_list.append(landuse_other_tuple)





def update_multiple_landuse_idx(records, buffer):
    
    try:
        cur_update, conn_update =uf. connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET landuse_idx5 = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()




def calc_landuse_index(id, geom_txt, buffer):
    
    
    global buffer_area
    global pixel_area
    
    global landuse_class_list
    landuse_class_num = len(landuse_class_list)
    
    
     
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM bbg2015_10m INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(bbg2015_10m.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query  GROUP BY (pvc).value;""", ([geom_txt, buffer]))

    result_set_1 = cur_1.fetchall()
    if  cur_1.rowcount==0:
        print('ways = ', id, ' is not overlapped with any land use classes!')
        return 0, 0, 0
    
       
    landuse_class_idx = 0.0
    prop_sum = 0.0
    land_prop_dict = {}
    
    
    for record in result_set_1:
        
        landuse_type  = record[0]
        landuse_count  = record[1]
    
        for i in range(landuse_class_num):
            landuse_classes = landuse_class_list[i]
            if landuse_type in landuse_classes:
                if i in land_prop_dict:
                    land_prop_dict[i]=  land_prop_dict[i]+ landuse_count
                else:
                    land_prop_dict[i]= landuse_count        
                break  
        
    
    for k in land_prop_dict:
        prop = land_prop_dict[k]*pixel_area/buffer_area
        land_prop_dict[k] = prop
        prop_sum = prop_sum + prop
        if prop > 0:
            landuse_class_idx = landuse_class_idx + prop * math.log(prop) / math.log(landuse_class_num)

    landuse_class_idx = - landuse_class_idx
    return landuse_class_idx, land_prop_dict, prop_sum



def calc(table, buffer):
    
    global buffer_area
    global pixel_area
    
    pixel_area = 10*10
    
    cur, conn = uf.connect()    
    cur.execute("select id, ST_AsText(ST_Transform(linestring, 28992)), ST_Area(st_buffer(ST_Transform(linestring, 28992), %s)) from " + source_table +" where visits_r is not null or visits_b is not null or visits_w is not null;", ([buffer]))
    record_result_set = cur.fetchall()

  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        geom_txt = row[1]
        buffer_area = row[2]
        
        
        result_one_tuple=()
        
        landuse_idx, land_prop_list, prop_sum = calc_landuse_index(id, geom_txt, buffer)
        if  landuse_idx < 0.0001 or abs(prop_sum - 1) >=  0.1 :
            #print("not normal landuse index for ways = ", int(id),  " landuse index = ", landuse_idx, " land_prop_list = ", land_prop_list, " prop_sum = ", prop_sum)
            logger.info('"not noraml landuse index for ways = ' + str(id) + " ; lanuse index number = " + str(landuse_idx) + "; prop_sum number = " + str(prop_sum) + " land_prop_list = "  +  str(land_prop_list).strip('[]'))
        
        if  landuse_idx < 0.0001:
            landuse_idx = 0.0
                                                 
        result_one_tuple = (landuse_idx, id)
        result_all_list.append(result_one_tuple)
        
        if row_index % uf.batch_number == 0:
            update_multiple_landuse_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc landuse for ways id ", int(id), "row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc landuse ways id "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time =time.time()
            result_all_list = []
    
    update_multiple_landuse_idx(result_all_list, buffer)
    cur.close()
    conn.close()