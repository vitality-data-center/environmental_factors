#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_crossing_p6
# Purpose:  Calculate the average crossing density for each pc6 area in The Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import psycopg2
import time 
import logging

import utility_fun2 as uf
logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table




def update_all_idx(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET crossing_num = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    


def calc_crossings(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    # skip connectivity=3; the 1-ways are typical for suburban areas,
    cur_1.execute("""select count(*) from crossings_2016 where ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s)) and edgecount !=3.0""", ([geom_txt, buffer]))
    
    result_set_1 = cur_1.fetchall()
    
    
    result = result_set_1[0]
    crossing_count = result[0]

       
    if crossing_count is None:
        logger.info('id = '+str(id)+' is not overlapped with any connectivity points!'+ " query result = " +str(result)) 
        return 0
    
     
    cur_1.close()
    conn_1.close()         
                      
    return crossing_count




def calc(table, buffer):
    
    total_start_time = time.time()
    
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
        connectivity = calc_crossings(id, center_geom_txt, buffer)


    
        result_one_tuple = (connectivity, id)
        result_all_list.append(result_one_tuple)
        
        
        if row_index % uf.batch_number == 0:
            
            update_all_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc connectivity index for pc6, id = ", int(id), " row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc connectivity index for pc6, id = "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time = time.time()
            result_all_list = []
    
    update_all_idx(result_all_list, buffer)
    cur.close()
    conn.close()
    
    total_end_time = time.time()
    total_time_diff = total_end_time - total_start_time # in seconds
    total_time_diff = round(total_time_diff/(60*60), 2)   # in hours
    print("finish calculating connectivity index...processed time in hours = ", time_diff," buffer size = ", buffer)
    logger.info("finish calculating connectivity index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))        
    
    

    
    
def test():   
    
    test_count1 = calc_crossings(11, 'POINT(135822.211 553186.417)', 20)
    print("test1 ", test_count1)
    test_count2 = calc_crossings(11, 'POINT(156888.476 516383.046)', 120)
    print("test2 ", test_count2)
    
#test()    