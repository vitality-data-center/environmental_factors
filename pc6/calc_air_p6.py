#!/usr/bin/python
#coding:utf-8

import utility_fun2 as uf
import psycopg2
import math
import time 
import logging

logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table




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
    

def calc_air_idx(id, geom_txt, buffer):
            
    global buffer_area
    
    cur_air, conn_air = uf.connect()
    cur_air.execute("""select no2_avg, pm25_avg, pm10_avg, ST_Area(ST_Intersection(geom,st_buffer(ST_GeomFromText(%s, 28992), %s))) 
    from air_pollution_nl_2015 where ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s));""", ([geom_txt, buffer, geom_txt, buffer]))
    
    result_set_air = cur_air.fetchall()
    if  cur_air.rowcount==0:
        print ('postcode pc6 = ', id), ' is not overlapped with any air pollution zones'
        return 0, 0, 0, 0
    
    no2_avg  = 0.0
    pm25_avg = 0.0
    pm10_avg = 0.0
    rate_sum = 0.0
    
    for record in result_set_air:
        # print "air index record = ", record
        no2  = record[0]
        pm25 = record[1]
        pm10 = record[2]
        intsec_area = record[3]
        rate = intsec_area/buffer_area
        # print "air index record = ", record, ' rate = ', rate
        
        no2_avg = no2_avg + no2 * rate
        pm25_avg = pm25_avg + pm25 * rate
        pm10_avg = pm10_avg + pm10 * rate
        rate_sum = rate_sum + rate

    cur_air.close()
    conn_air.close()          
                      
    return no2_avg, pm25_avg, pm10_avg, rate_sum

  
def calc_nox_idx(id, geom_txt, buffer):
        
    global buffer_area
    
    cur_nox, conn_nox = uf.connect()
    cur_nox.execute("""select nox_avg, ST_Area(ST_Intersection(geom,st_buffer(ST_GeomFromText(%s, 28992), %s)))
    from nox_nl_2km_2015 where ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s));""", ([geom_txt, buffer, geom_txt, buffer]))
    
    result_set_nox = cur_nox.fetchall()
    if  cur_nox.rowcount==0:
        print ('postcode pc6 = ', id), ' is not overlapped with any nox zones'
        return 0, 0
    
    nox_avg  = 0.0
    rate_sum = 0.0
    
    for record in result_set_nox:
        # print "nox record = ", record
        nox  = record[0]
        intsec_area = record[1]
        rate = intsec_area/buffer_area
        # print "nox record = ", record, " ; rate = ", rate
        
        nox_avg = nox_avg + nox * rate
        rate_sum = rate_sum + rate

    cur_nox.close()
    conn_nox.close()          
                      
    return nox_avg, rate_sum





def calc(table, buffer):
    
    global buffer_area
    buffer_area = math.pi*buffer*buffer
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
        no2_avg, pm25_avg, pm10_avg, rate_sum1 = calc_air_idx(id, center_geom_txt, buffer)
        nox_avg, rate_sum2 = calc_nox_idx(id, center_geom_txt, buffer)
    
        if  rate_sum1 - 1 > 0.0001 or rate_sum2 - 1 > 0.0001:
            print("something wrong with calc air index for pc6 = ", id,  rate_sum1, rate_sum2)
            print("pc6 = ", id, "; area = ", buffer_area , "; no2_avg = ", no2_avg, "; nox_avg = ", nox_avg, "; pm25_avg = ", pm25_avg , "; pm10_avg = ", pm10_avg)
            logger.info('"something wrong with calc air index for pc6 = ' + str(id) + " ; sum1 = " + str(rate_sum1) + " ; sum2 = " + str(rate_sum2))
            logger.info("pc6 id = " + str(id) + "; area = " +  str(buffer_area) + "; no2_avg = " +  str(no2_avg) + "; nox_avg = " + str(nox_avg) + "; pm25_avg = " + str(pm25_avg) + "; pm10_avg = " + str(pm10_avg))
        
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
            start_time =time.time()
            result_all_list = []
    
    update_all_air_idx(result_all_list, buffer)
    cur.close()
    conn.close()        
    
    

    
    
    