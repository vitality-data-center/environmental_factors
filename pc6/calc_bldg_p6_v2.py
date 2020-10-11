#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     calc_bldg_p6_v2
# Purpose:  Calculate the average building density for each pc6 area in The Netherlands
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
import math

import utility_fun2 as uf
logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table
variable = 'commercial building'
commcercial_function = ['industriefunctie', 'winkelfunctie', 'onderwijsfunctie', 'bijeenkomstfunctie', 'gezondheidszorgfunctie', 'kantoorfunctie', 'logiesfunctie', 'sportfunctie', 'overige gebruiksfunctie']
residential_function = ['woonfunctie']

def update_all_idx(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) + " SET comm_bldg_ratio = %s, resi_bldg_ratio = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    





def calc_bldg_area(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    # skip connectivity=3; the 1-ways are typical for suburban areas,
    cur_1.execute("""with bldg_query as (
      select * from pand_dec_2016 where ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s))
    )
    select a.gid, max(st_area(a.geom)), string_agg(b.gebruiksdo::text, ';') from bldg_query a, vobject_dec_2016 b where ST_Intersects(a.geom, b.geom) group by a.gid""", ([geom_txt, buffer]))
    
    
    total_commercial_area = 0
    total_residential_area = 0
    result_set_1 = cur_1.fetchall()
    
    # 
    if len(result_set_1)==0:
        logger.info('id = '+str(id)+' is not overlapped with any '+ variable +' features!')
        return 0, 0;
        
    for record in result_set_1:
        bldg_gid = record[0]
        bldg_area = record[1]
        bldg_functions = record[2]
        if bldg_functions == None:
            #logger.info('postcode id  = '+str(gid)+' has buildings with not functions')
            continue 
        bldg_functions = bldg_functions.split(";")
        bldg_function_set = set(bldg_functions)
        
        # check if commercial 
        if is_desired(bldg_function_set, commcercial_function):
            total_commercial_area = total_commercial_area + bldg_area
        
        # check if residential
        if is_desired(bldg_function_set, residential_function):
            total_residential_area = total_residential_area + bldg_area     
    
     
    cur_1.close()
    conn_1.close()         
                      
    return total_commercial_area, total_residential_area


def is_desired(function_set, desired_fun):
    desired = False
    for f in desired_fun:
        if f in  function_set:
            return True
    return desired  
    
    

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
        comm_bldg_area, resi_bldg_area = calc_bldg_area(id, center_geom_txt, buffer)
        
        buffer_area = math.pi*buffer*buffer
        if comm_bldg_area > buffer_area:
            logger.info("calc "+ variable +" index for pc6, id = "+ str(int(id))+" has commercial areas =  "+ str(comm_bldg_area) + " larger than buffer"+ str(buffer_area))
            comm_bldg_area = buffer_area

        if resi_bldg_area > buffer_area:
            logger.info("calc "+ variable +" index for pc6, id = "+ str(int(id))+" has residential areas =  "+ str(resi_bldg_area) + " larger than buffer"+ str(buffer_area))
            resi_bldg_area = buffer_area

    
    
        comm_bldg_ratio = comm_bldg_area / buffer_area
        resi_bldg_ratio = resi_bldg_area / buffer_area
    
    
        result_one_tuple = (comm_bldg_ratio, resi_bldg_ratio , id)
        result_all_list.append(result_one_tuple)
        
        
        if row_index % uf.batch_number == 0:
            
            update_all_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc "+ variable +" index for pc6, id = ", int(id), " row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc "+ variable +" index for pc6, id = "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
            # empty list
            start_time = time.time()
            result_all_list = []
    
    update_all_idx(result_all_list, buffer)
    cur.close()
    conn.close()
    
    total_end_time = time.time()
    total_time_diff = total_end_time - total_start_time # in seconds
    total_time_diff = round(total_time_diff/(60*60), 2)   # in hours
    print("finish calculating " + variable +" index...processed time in hours = ", time_diff," buffer size = ", buffer)
    logger.info("finish calculating "+ variable +" index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))        
    
    

    
    
def test():   
    uf.init_logger()
    
    #area1 = calc_bldg_area(11, 'POINT(140348.973 455290.836)', 50)
    #print area1
    
    #area2 = calc_bldg_area(11, 'POINT(140399.017 455928.084)', 180)
    #print area2
    
    area3 = calc_bldg_area(11, 'POINT(139848.727 454360.278)', 0.5)
    print(area3)
    
    area4 = calc_bldg_area(12, "POINT(137877.494272554 457895.100128259)", 100)
    print(area4[1]/float(math.pi*100*100))

    
#test()    