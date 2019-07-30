import utility_fun as uf
import math
import psycopg2
import time
import logging

logger = logging.getLogger("vdc_logger")
source_table = uf.source_table # nl_pc6 table
batch_num = uf.batch_number
target_table = uf.target_table
buffer_area = 0; 


def update_multiple_street_idx(records, buffer):
    
    try:
        cur_update, conn_update = uf.connect()
        cur_update.executemany("UPDATE " +target_table+"_"+ str(buffer) +" set street_length = %s, street_density = %s, motorway_length = %s, motorway_density = %s where id = %s;",
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()


def calc_street_length(id, geom_txt, buffer):
    
  
    cur1, conn1 = uf.connect()
    cur1.execute("""   
        SELECT sum(ST_Length(ST_Intersection(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))::geography)) AS street_length
        FROM ways
        WHERE ST_Intersects(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))
        and  (tags -> 'highway' IS NOT NULL) 
        and (tags -> 'highway'= 'primary' or tags -> 'highway'= 'primary_link' or tags -> 'highway'= 'trunk' or tags -> 'highway' ='trunk_link' or tags -> 'highway' ='secondary' or tags -> 'highway'= 'secondary_link' or tags -> 'highway' ='tertiary' or tags -> 'highway'= 'tertiary_link' or tags -> 'highway' ='footway' or tags -> 'highway' ='path' or tags -> 'highway' ='steps' or tags -> 'highway' ='pedestrian' or tags -> 'highway' ='living_street' or tags -> 'highway' ='track' or tags -> 'highway'= 'residential' or tags -> 'highway' ='service' or tags -> 'highway' ='unclassified' or tags -> 'highway'= 'road' or tags -> 'highway' ='cycleway' ) 
        and ST_IsEmpty(linestring) IS false 
        AND ST_IsValid(linestring) IS true
        and (not (ways.tags::hstore ? 'maxspeed') or convert_to_integer(tags->'maxspeed') < 50) 
        and (not (ways.tags::hstore ? 'maxspeed:forward') or convert_to_integer(tags->'maxspeed:forward') < 50) 
        and (not (ways.tags::hstore ? 'maxspeed:backward') or convert_to_integer(tags->'maxspeed:backward') < 50);""", ([geom_txt, buffer, geom_txt, buffer]));
    
        # SELECT sum(ST_Length(ST_Intersection(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))::geography)) AS street_length, json_agg(ways.id::text), count(*)
        # SELECT sum(ST_Length(ways.linestring::geography)) AS street_length, json_agg(ways.id::text)
        # and (not (ways.tags::hstore ? 'maxspeed:backward') or convert_to_integer(tags->'maxspeed:backward') < 50);""", ([geom_txt, buffer]));
        # too slow with --WHERE ST_Intersects(ST_Transform(ways.linestring, 28992), ST_Buffer(ST_SetSRID(ST_Point(%s, %s), 28992), %s))
    
    result_set1 = cur1.fetchall()
    if  cur1.rowcount==0:
        print ('id = ', id), ' is not overlapped with any streets'
        return id, -2
    
    result = result_set1[0]
    street_length = result[0]
        #osm_ids = result[1]
        #street_num = result[2]
    
    
    if  street_length is None:
        print ('id = ', id, " query result = ", result), ' is not overlapped with any streets'
        return id, 0
    

    cur1.close()
    conn1.close()
            
    return id, street_length
    
    


def calc_motorway_length(id, geom_txt, buffer):
    
    try:
        cur1, conn1 = uf.connect()
        cur1.execute("""    
        SELECT sum(ST_Length(ST_Intersection(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))::geography)) AS motorway_length
        FROM ways
        WHERE ST_Intersects(ways.linestring, ST_Transform(ST_Buffer(ST_GeomFromText(%s, 28992), %s), 4326))
        and (tags -> 'highway' IS NOT NULL) 
        and (tags -> 'highway'= 'primary' or tags -> 'highway'= 'primary_link' or tags -> 'highway'= 'trunk' or tags -> 'highway' ='trunk_link' or tags -> 'highway' ='secondary' or tags -> 'highway'= 'secondary_link' or tags -> 'highway' ='tertiary' or tags -> 'highway'= 'tertiary_link' or tags -> 'highway' ='motorway'
        or tags -> 'highway' ='motorway_link') 
        and ST_IsEmpty(linestring) IS false 
        AND ST_IsValid(linestring) IS true
        and ((convert_to_integer(tags->'maxspeed') >= 50) 
        or (convert_to_integer(tags->'maxspeed:forward') >= 50) 
        or (convert_to_integer(tags->'maxspeed:backward') >= 50));""", ([geom_txt, buffer, geom_txt, buffer]));

        #or (convert_to_integer(tags->'maxspeed:backward') >= 50));""", ([geom_txt, buffer, geom_txt, buffer]));    
        # too slow with --WHERE ST_Intersects(ST_Transform(ways.linestring, 28992), ST_Buffer(ST_SetSRID(ST_Point(%s, %s), 28992), %s))
    
        result_set1 = cur1.fetchall()
        if  cur1.rowcount==0:
            print ('id = ', id), ' is not overlapped with any motorways'
            return id, -2
    
        result = result_set1[0]
        motorway_length = result[0]
        # osm_ids = result[1]
        # motorway_num = result[2]
    
        if  motorway_length is None:
            print ('id = ', id, " query result = ", result), ' is not overlapped with any motorways'
            return id, 0
    
    except (Exception, psycopg2.Error) as error:
        print("error in caclculating motorway length".format(error))
        logger.info("error in caclculating motorway length, for pc6 " + str(id))
    finally:
        # closing database connection.
        if (conn1):
            cur1.close()
            conn1.close()
    
    return id, motorway_length
    
    
    

def calc(table, buffer):
    
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
        id, street_length = calc_street_length(id, center_geom_txt, buffer)
        id, motorway_length = calc_motorway_length(id, center_geom_txt, buffer)
        
        street_density = street_length/buffer_area
        motorway_density = motorway_length/buffer_area
        
        result_one_tuple = (street_length, street_density, motorway_length, motorway_density, id)
        result_all_list.append(result_one_tuple)   
        
        
        if row_index % batch_num == 0:
            
            update_multiple_street_idx(result_all_list, buffer)            
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2)
            print "street index for pc6 id = ", int(id), " row_index" , row_index, " result= " , result_one_tuple, "; running time = ", time_diff_min, " mins! ", time_diff, " sec! ", " buffer = ", buffer
            logger.info("street index for pc6 id = "+ str(int(id))+  " row_index" + str(row_index) + " result= " +  str(result_one_tuple) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff) + " sec! buffer = " + str(buffer))
            # empty list
            start_time =time.time()
            result_all_list = []
            
            
    update_multiple_street_idx(result_all_list, buffer)
    cur.close()
    conn.close()         