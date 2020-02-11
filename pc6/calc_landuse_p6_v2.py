#!/usr/bin/python
#coding:utf-8
import psycopg2
import string
import sys
import re
import math
import logging
import time
import utility_fun as uf


logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table





landuse_class_list = []
landuse_residential_tuple=(20, )
landuse_recreational_tuple=(40, 41, 42, 43, 44, 50, 51, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 83)
landuse_other_tuple=(10, 11, 12, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35)
landuse_class_list.append(landuse_residential_tuple)
landuse_class_list.append(landuse_recreational_tuple)
landuse_class_list.append(landuse_other_tuple)








def update_multiple_landuse_idx(records, buffer):
    
    try:
        cur_update, conn_update =uf. connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET landuse_idx = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()






def calc_landuse_prop(pc6_id, center_geom_txt, landuse_classes, buffer):
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""select id, bg2015, st_area(ST_Intersection(geom, st_buffer(ST_GeomFromText(%s, 28992), %s))), st_area(st_buffer(ST_GeomFromText(%s, 28992), %s)) 
    from bbg2015 where bg2015 in %s and ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s));""", ([center_geom_txt, buffer, center_geom_txt, buffer, landuse_classes, center_geom_txt, buffer]))

    result_set_1 = cur_1.fetchall()
    if  cur_1.rowcount==0:
        #print ('postcode = ', pc6_id), ' is not overlapped with any land use classes', landuse_classes
        return 0 
    
    landuse_area_sum  = 0.0
    for record in result_set_1:
        landuse_id  = record[0]
        landuse_type  = record[1]
        landuse_area = record[2]
        buffer_area = record[3]
        # print "landuse_id = ", landuse_id, " ; landuse_type = ", landuse_type, " landuse_area = ", landuse_area, " ; buffer_area = ", buffer_area, " ; prop = ", landuse_area / buffer_area
        landuse_area_sum = landuse_area_sum + landuse_area


    cur_1.close()
    conn_1.close()  
    
    landuse_prop = landuse_area_sum / buffer_area     
                      
    return  landuse_prop




def calc_landuse_index(pc6_id, center_geom_txt, buffer):
    
    global landuse_class_list
    landuse_class_num = len(landuse_class_list)
    
    landuse_class_idx = 0.0
    prop_sum = 0.0
    land_prop_list = []
    
    for i in range(len(landuse_class_list)):
        landuse_classes = landuse_class_list[i]
        prop = calc_landuse_prop(pc6_id, center_geom_txt, landuse_classes, buffer)
        prop_sum = prop_sum +prop
        land_prop_turple = (i, prop)
        land_prop_list.append(land_prop_turple)
        if prop > 0:
            landuse_class_idx = landuse_class_idx + prop * math.log(prop) / math.log(landuse_class_num)

    landuse_class_idx = - landuse_class_idx
    return landuse_class_idx, land_prop_list, prop_sum





def calc(table, buffer):
    
    cur, conn = uf.connect()    
    cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + source_table +";")
    record_result_set = cur.fetchall()
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        center_geom_txt = row[1]
        
        result_one_tuple=()
        
        landuse_idx, land_prop_list, prop_sum = calc_landuse_index(id, center_geom_txt, buffer)
        #if  landuse_idx < 0.1 or abs(prop_sum - 1) > 0.001: 
        #    print "not normal landuse index for pc6 = ", int(id),  " landuse index = ", landuse_idx, " land_prop_list = ", land_prop_list, " prop_sum = ", prop_sum
        #    logger.info('"not noraml landuse index for pc6 = ' + str(id) + " ; lanuse index number = " + str(landuse_idx) + "; prop_sum number = " + str(prop_sum) + " land_prop_list = "  +  str(land_prop_list).strip('[]'))
                         
                         
        result_one_tuple = (landuse_idx, id)
        result_all_list.append(result_one_tuple)
        
        if row_index % uf.batch_number == 0:
            update_multiple_landuse_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print "calc landuse for pc6 id ", int(id), "row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer
            logger.info("calc landuse for pc6 id "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time =time.time()
            result_all_list = []
    
    update_multiple_landuse_idx(result_all_list, buffer)
    cur.close()
    conn.close()