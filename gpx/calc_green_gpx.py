#!/usr/bin/python
#coding:utf-8


import utility_fun2 as uf
import math
import psycopg2
import time
import logging
logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table





def update_all_ndvi(records):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+ " SET green_300 = %s, green_600 = %s, green_1000 = %s, green_1500 = %s where id = %s",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()


def calc_ndvi_avg(id, geom_txt):
    
    green_300 = calc_ndvi_buffer(id, geom_txt, 300)
    green_600 = calc_ndvi_buffer(id, geom_txt, 600)
    green_1000 = calc_ndvi_buffer(id, geom_txt, 1000)
    green_1500 = calc_ndvi_buffer(id, geom_txt, 1500)
    
    return green_300, green_600, green_1000, green_1500, id

    

def calc_ndvi_buffer(id, geom_txt, buffer):

    
    cur5, conn5 = uf.connect()
    cur5.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM ndvi_2015_30m INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(ndvi_2015_30m.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set5 = cur5.fetchall()
    if  cur5.rowcount==0:
        print ('id = ', id), ' is not overlapped with any green areas'
        return id, -99999
    
    total_num = 0
    total_value = 0
    ndvi_avg = -99999
    for record in result_set5:
        #print "noise record = ", record
        ndvi_value = record[0]
        ndvi_count = record[1]
        
        if ndvi_value > 0 :
            total_num =  total_num + ndvi_count
            total_value = total_value + ndvi_value * ndvi_count
            
            
    if total_num == 0:
        logger.info("track workout_id = "+ str(id) + " has no green value!")
        return id, ndvi_avg    
        
    #print('avg = ', ndvi_avg, total_value, total_num)     

    ndvi_avg = total_value / total_num
    
       
    cur5.close()
    conn5.close()         
                      
    return ndvi_avg




def calc():
    
    global buffer_area
    global pixel_area

    
    cur, conn = uf.connect()    
    cur.execute("select id, ST_AsText(ST_Transform(geom, 28992)) from " + source_table +" where green_300 is null;")
    record_result_set = cur.fetchall()
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        geom_txt = row[1]
        
        result_one_tuple=()
        green_300, green_600, green_1000, green_1500, id = calc_ndvi_avg(id, geom_txt)
        result_one_tuple = (green_300, green_600, green_1000, green_1500, id)
        result_all_list.append(result_one_tuple)
        
        if row_index % uf.batch_number == 0:
            update_all_ndvi(result_all_list)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc ndvi green for id ", int(id), "row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec!")
            logger.info("calc ndvi green for id "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec!")
        
            # empty list
            start_time =time.time()
            result_all_list = []
    
    update_all_ndvi(result_all_list)
    cur.close()
    conn.close()