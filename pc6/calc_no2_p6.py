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
variable_name = 'no2_2011'




def update_all_records(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET no2_2011 = %s where id = %s",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    

def calc_no2_avg(id, geom_txt, buffer):

    
    cur_noise, conn_noise = uf.connect()
    cur_noise.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM no2_2011_100m INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(no2_2011_100m.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set = cur_noise.fetchall()
    if  cur_noise.rowcount==0:
        print ('id = ', id), ' is not overlapped with any no2 polluted areas'
        return id, -99999
    
    total_num = 0
    total_value = 0
    no2_avg = -99999
    for record in result_set:
        #print "noise record = ", record
        no2_value = record[0]
        no2_count = record[1]
        
        if no2_value > 0 :
            total_num =  total_num + no2_count
            total_value = total_value + no2_value * no2_count
            
            
        if total_num == 0:
            logger.info("pc6 gid = "+ str(id) + " has no no2 value !")
            return id, -99999    
        
    #print('avg = ', ndvi_avg, total_value, total_num)     

    no2_avg = total_value / total_num
    
       
    cur_noise.close()
    conn_noise.close()         
                      
    return id, no2_avg




def calc(table, buffer):
    
    total_start_time = time.time()
    
    global buffer_area
    global pixel_area

    
    cur, conn = uf.connect()    
    cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + source_table +" where postcode = '8822WV';")
    #cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + source_table +";")
    record_result_set = cur.fetchall()
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        center_geom_txt = row[1]
        
        result_one_tuple=()
        id, no2_avg = calc_no2_avg(id, center_geom_txt, buffer)
        result_one_tuple = (no2_avg, id)
        result_all_list.append(result_one_tuple)
        
        if row_index % uf.batch_number == 0:
            update_all_records(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc "+ variable_name + " for pc6 id ", int(id), " row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc " + variable_name +" for pc6 id "+ str(int(id)) +  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time =time.time()
            result_all_list = []
    
    update_all_records(result_all_list, buffer)
    cur.close()
    conn.close()
    
    total_end_time = time.time()
    total_time_diff = total_end_time - total_start_time # in seconds
    total_time_diff = round(total_time_diff/(60*60), 2)   # in hours
    print("finish calculating " + variable_name +" index...processed time in hours = ", total_time_diff," buffer size = ", buffer)
    logger.info("finish calculating "+ variable_name +" index...processed time in hours= "+ str(total_time_diff) + " buffer size = " + str(buffer))        
    
    