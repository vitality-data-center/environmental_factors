#!/usr/bin/python
#coding:utf-8


import psycopg2
import time 
import logging
import utility_fun2 as uf
logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table




def update_all_idx(records):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table + " SET crossing1_300 = %s, crossing3_300 = %s, crossing4plus_300 = %s, crossing1_600 = %s, crossing3_600 = %s, crossing4plus_600 = %s, crossing1_1000 = %s, crossing3_1000 = %s, crossing4plus_1000, crossing1_1500 = %s, crossing3_1500 = %s, crossing4plus_1500 = %s where id = %s;",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()
    

def calc_crossings(gid, geom_txt):
    
    crossing1_300, crossing3_300, crossing4plus_300 = calc_crossings_buffer(id, geom_txt, 300)
    crossing1_600, crossing3_600, crossing4plus_600 = calc_crossings_buffer(id, geom_txt, 600)
    crossing1_1000, crossing3_1000, crossing4plus_1000 = calc_crossings_buffer(id, geom_txt, 1000)
    crossing1_1500, crossing3_1500, crossing4plus_1500 = calc_crossings_buffer(id, geom_txt, 1500)
    
    return crossing1_300, crossing3_300, crossing4plus_300, crossing1_600, crossing3_600, crossing4plus_600, crossing1_1000, crossing3_1000, crossing4plus_1000, crossing1_1500, crossing3_1500, crossing4plus_1500, id


def calc_crossings_buffer(gid, geom_txt, buffer):

    
    cur_1, conn_1 = uf.connect()
    # skip connectivity=3; the 1-ways are typical for suburban areas,
    cur_1.execute("""select edgecount from crossings_2016 where ST_Intersects(geom, st_buffer(ST_GeomFromText(%s, 28992), %s))""", ([geom_txt, buffer]))
    
    result_set_1 = cur_1.fetchall()

    count_1 = 0   
    count_3 = 0   
    count_4plus = 0
    
    if len(result_set_1)==0:
        logger.info('id = '+str(id)+' is not overlapped with any connectivity points!'+ " query result = " +str(result_set_1)) 
        return 0, 0, 0
    
    for record in result_set_1:
        edge_count = record[0]
        if edge_count == 1:
            count_1 += 1
        elif edge_count ==3:
            count_3 += 1
        else:
            count_4plus +=1
     
    cur_1.close()
    conn_1.close()         
                      
    return count_1, count_3,  count_4plus




def calc():
    
    total_start_time = time.time()
    
    cur, conn = uf.connect()
    cur.execute("select id, ST_AsText(ST_Transform(geom, 28992)) from " + source_table +" where crossing1_300 is null;")
    record_result_set = cur.fetchall()
  
  
    result_all_list = []
  
    start_time =time.time()
    row_index = 0
    for row in record_result_set:
        row_index = row_index + 1
        id = row[0]
        geom_txt = row[1]
        
        result_one_tuple=()
        crossing1_300, crossing3_300, crossing4plus_300, crossing1_600, crossing3_600, crossing4plus_600, crossing1_1000, crossing3_1000, crossing4plus_1000, crossing1_1500, crossing3_1500, crossing4plus_1500, id  = calc_crossings(id, geom_txt)
        result_one_tuple = (crossing1_300, crossing3_300, crossing4plus_300, crossing1_600, crossing3_600, crossing4plus_600, crossing1_1000, crossing3_1000, crossing4plus_1000, crossing1_1500, crossing3_1500, crossing4plus_1500, id)
        result_all_list.append(result_one_tuple)
        
        
        if row_index % uf.batch_number == 0:
            
            update_all_idx(result_all_list, buffer)
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("calc connectivity index for , id = ", int(id), " row_index = " , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer)
            logger.info("calc connectivity index for, id = "+ str(int(id))+  " row_index = " + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
        
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
    
    test_count1 = calc_crossings(11, 'POINT(135822.211 553186.417)')
    print("test1 ", test_count1)
    test_count2 = calc_crossings(11, 'POINT(156888.476 516383.046)')
    print("test2 ", test_count2)
    
#test()    