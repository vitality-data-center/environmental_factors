#-------------------------------------------------------------------------------
# Name:     calc_bldg_p6
# Purpose:  Calculate the average building density for each pc6 area in The Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import utility_fun2 as uf
import math
import psycopg2
import time
import logging

logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table
buffer_area = 0; 


def update_multiple_bldg_idx(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) +" set res_bldg_area = %s, res_bldg_density = %s where id = %s;",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()




def calc_bldg_area(id, geom_txt, buffer):
    cur1, conn1 = uf.connect()
    cur1.execute("""
    SELECT sum(st_area(ST_MakePolygon(ways.linestring)::geography)) AS res_bldg_area
    FROM ways
    WHERE ST_Intersects(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))
    and (ways.tags -> 'building'= 'apartments' or ways.tags -> 'building'= 'farm' or ways.tags -> 'building'= 'house' or ways.tags -> 'building'= 'detached' or ways.tags -> 'building'= 'residential' or ways.tags -> 'building'= 'dormitory' or ways.tags -> 'building'= 'terrace' or ways.tags->'building'='houseboat' or ways.tags->'building'='bungalow' or ways.tags->'building'='static_caravan' or ways.tags->'building'='cabin')
    and ST_IsEmpty(ways.linestring) IS false 
    AND ST_IsValid(ways.linestring) IS true
    and ST_IsClosed(ways.linestring);""", ([geom_txt, buffer]));
    
    # too slow with --WHERE ST_Intersects(ST_Transform(ways.linestring, 28992), ST_Buffer(ST_SetSRID(ST_Point(%s, %s), 28992), %s))
    # SELECT sum(st_area(ST_MakePolygon(ways.linestring)::geography)) AS res_bldg_area, json_agg(ways.id::text)
    result_set1 = cur1.fetchall()
    if  cur1.rowcount==0:
        print ('id = ', id), ' is not overlapped with any buildings'
        return id, -2
    
    result = result_set1[0]
    bldg_area = result[0]
    #osm_ids = result[1]
    #print 'pc6 id = ', id, " ; osm ids = ", osm_ids
    
    
       
    if bldg_area is None:
        logger.info('id = '+str(id)+' is not overlapped with any streets!'+ " query result = " +str(result)) 
        return id, 0
    
    cur1.close()
    conn1.close()
    
    return id, bldg_area
    
    


def calc(table, buffer):
    
    buffer_area = math.pi*buffer*buffer
    
    cur, conn = uf.connect()    
    cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + source_table +";")
    # cur.execute("select gid, ST_AsText(st_centroid(geom)) from " + source_table +" where gid = 177037;")

    record_result_set = cur.fetchall()
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        center_geom_txt = row[1]
        
        result_one_tuple=()   
        id, bldg_area = calc_bldg_area(id, center_geom_txt, buffer)
        bldg_density = bldg_area/buffer_area
        result_one_tuple = (bldg_area, bldg_density, id)
        result_all_list.append(result_one_tuple)   
        if row_index % batch_num == 0:

            update_multiple_bldg_idx(result_all_list, buffer)            
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2)
            print("calc bldg for pc6 id ", int(id), "row_index" , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc bldg for pc6 id "+ str(int(id))+  " row_index" + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
            # empty list
            start_time =time.time()
            result_all_list = []
            
            
    update_multiple_bldg_idx(result_all_list, buffer)
    cur.close()
    conn.close()         