#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_air_p6_v2
# Purpose:  Calculate the average air pollution concentrations for each pc6 area in The Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import utility_fun2 as uf
import psycopg2
import time 
import logging

logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table


def check_skip(gid, buffer):
    cur_5, conn_5 = uf.connect()
    cur_5.execute("""select no2_avg notnull from """ +target_table+"_"+ str(buffer) + """ where id = %s """, ([gid]))
    
    result_set5 = cur_5.fetchall()
    result = result_set5[0]
    flag = result[0]
    
    skipped = True
    
    if flag:
        skipped = True
    else:
        skipped = False 

    cur_5.close()
    conn_5.close()
    
    return skipped




def update_all_air_idx(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET no2_avg = %s, nox_avg = %s, pm25_avg = %s, pm10_avg = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    


def calc_no2_avg(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM no2_2009 INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(no2_2009.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set = cur_1.fetchall()

    
    total_num = 0
    total_value = 0
    default_avg = -99999
    for record in result_set:
        
        value = record[0]
        count = record[1]
        total_num =  total_num + count
        total_value = total_value + value * count
    
    if total_num == 0:
        logger.info("pc6 gid = "+ str(gid) + " has no no2 value!")
        return gid, default_avg
        
    avg_value = total_value / total_num

    #print('avg = ', avg_value, total_value, total_num)        
    cur_1.close()
    conn_1.close()         
                      
    return gid, avg_value

def calc_nox_avg(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM nox_2009 INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(nox_2009.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set = cur_1.fetchall()
    
    total_num = 0
    total_value = 0
    default_avg = -99999
    for record in result_set:
        value = record[0]
        count = record[1]
        total_num =  total_num + count
        total_value = total_value + value * count
    
    if total_num == 0:
        logger.info("pc6 gid = "+ str(gid) + " has no nox value!")
        return gid, default_avg
        
    avg_value = total_value / total_num

    #print('avg = ', avg_value, total_value, total_num)        
    cur_1.close()
    conn_1.close()         
                      
    return gid, avg_value


def calc_pm25_avg(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM pm25_2009 INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(pm25_2009.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set = cur_1.fetchall()

    
    total_num = 0
    total_value = 0
    default_avg = -99999
    for record in result_set:
        value = record[0]
        count = record[1]
        total_num =  total_num + count
        total_value = total_value + value * count
    
    if total_num == 0:
        logger.info("pc6 gid = "+ str(gid) + " has no pm25 value!")
        return gid, default_avg
        
    avg_value = total_value / total_num

    #print('avg = ', avg_value, total_value, total_num)        
    cur_1.close()
    conn_1.close()         
                      
    return gid, avg_value


def calc_pm10_avg(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    cur_1.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM pm10_2009 INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(pm10_2009.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set = cur_1.fetchall()

    
    total_num = 0
    total_value = 0
    default_avg = -99999
    for record in result_set:
        value = record[0]
        count = record[1]
        total_num =  total_num + count
        total_value = total_value + value * count
    
    if total_num == 0:
        logger.info("pc6 gid = "+ str(gid) + " has no pm10 value!")
        return gid, default_avg
        
    avg_value = total_value / total_num

    #print('avg = ', avg_value, total_value, total_num)        
    cur_1.close()
    conn_1.close()         
                      
    return gid, avg_value






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
        
        if check_skip(id, buffer):
            continue
        
        
        result_one_tuple=()
        id, no2_avg = calc_no2_avg(id, center_geom_txt, buffer)
        id, nox_avg = calc_nox_avg(id, center_geom_txt, buffer)
        id, pm25_avg = calc_pm25_avg(id, center_geom_txt, buffer)
        id, pm10_avg = calc_pm10_avg(id, center_geom_txt, buffer)

    
        result_one_tuple = (no2_avg, nox_avg, pm25_avg, pm10_avg, id)
        result_all_list.append(result_one_tuple)
        
        
        if row_index % uf.batch_number == 0:
            
            update_all_air_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc air pollution index for pc6, id = ", int(id), " row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc air pollution index for pc6, id = "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time = time.time()
            result_all_list = []
    
    update_all_air_idx(result_all_list, buffer)
    cur.close()
    conn.close()        
    
    

    
    
    