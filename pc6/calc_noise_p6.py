#!/usr/bin/python
#coding:utf-8


import utility_fun as uf
import math
import psycopg2
import time
import logging
logger = logging.getLogger("vdc_logger")




buffer_area = 0.0; 
pixel_area = 0.0; 

def update_all_noise(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +uf.target_table+"_"+ str(buffer) + " SET dn_1 = %s, dn_2 = %s, dn_3 = %s, dn_4 = %s, dn_5 = %s, dn_6 = %s where id = %s",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    

def calc_noise_distributions(id, geom_txt, buffer):
    
    global buffer_area
    global pixel_area
    
    cur_noise, conn_noise = uf.connect()
    cur_noise.execute("""with clip_query as (SELECT ST_ValueCount(ST_Clip(st_union(rast), clipper.geom, false)) as pvc
     FROM noise_2016_10m INNER JOIN 
     (select st_buffer(ST_GeomFromText(%s, 28992), %s) as geom) AS clipper 
     ON ST_Intersects(noise_2016_10m.rast, clipper.geom) GROUP BY clipper.geom)
     SELECT (pvc).value, SUM((pvc).count) As total from clip_query GROUP BY (pvc).value;""", ([geom_txt, buffer]))
    
    result_set_noise = cur_noise.fetchall()
    if  cur_noise.rowcount==0:
        print ('id = ', id), ' is not overlapped with any noise areas'
        return id, -2, -2, -2, -2, -2, -2
    
    dn_1 = 0.0 
    dn_2 = 0.0 
    dn_3 = 0.0     
    dn_4 = 0.0 
    dn_5 = 0.0 
    dn_6 = 0.0 
    
    for record in result_set_noise:
        #print "noise record = ", record
        noise_dn = record[0]
        noise_count = record[1]
        if noise_dn == 1:
            dn_1 = noise_count*pixel_area/buffer_area
        elif noise_dn == 2:
            dn_2 = noise_count*pixel_area/buffer_area
        elif noise_dn == 3:
            dn_3 = noise_count*pixel_area/buffer_area    
        elif noise_dn == 4:
            dn_4 = noise_count*pixel_area/buffer_area    
        elif noise_dn == 5:
            dn_5 = noise_count*pixel_area/buffer_area                    
        else:
            dn_6 = dn_6 + noise_count*pixel_area/buffer_area
            
    cur_noise.close()
    conn_noise.close()         
                      
    return id, dn_1, dn_2, dn_3, dn_4, dn_5, dn_6




def calc(table, buffer):
    
    global buffer_area
    global pixel_area
    
    buffer_area = math.pi*buffer*buffer
    pixel_area = 10*10
    
    cur, conn = uf.connect()    
    cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + uf.source_table +";")
    record_result_set = cur.fetchall()
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        center_geom_txt = row[1]
        
        result_one_tuple=()
        id, dn_1, dn_2, dn_3, dn_4, dn_5, dn_6 = calc_noise_distributions(id, center_geom_txt, buffer)
        dn_sum = dn_1 + dn_2 +  dn_3 + dn_4 + dn_5 + dn_6 
        if  dn_sum - 1 > 0.0001:
            print "something wrong with calc area for pc6 ", id, "; dn_sum = ", dn_sum
    
        result_one_tuple = (dn_1, dn_2, dn_3, dn_4, dn_5, dn_6, id)
        result_all_list.append(result_one_tuple)
        
        if row_index % uf.batch_number == 0:
            update_all_noise(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print "calc noise for pc6 id ", int(id), "row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer
            logger.info("calc noise for pc6 id "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time =time.time()
            result_all_list = []
    
    update_all_noise(result_all_list, buffer)
    cur.close()
    conn.close()