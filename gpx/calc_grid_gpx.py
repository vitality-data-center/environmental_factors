#-------------------------------------------------------------------------------
# Name:     calc_grid_gpx
# Purpose:  Calculate the nubmer of sport tracks for each cell in the Netherlands
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from __future__ import division
import gdal, ogr, struct
import numpy as np
import math
import os
import utility_fn as uf
import logging
from osgeo import ogr
import time
import psycopg2


linux_mode = True
test_mode = False

uf.init_logger(linux_mode)
logger = logging.getLogger("vdc_logger")

pixel_size = 25
NoData_value = -9999
batch_number = 1000

grid_table = 'grid_table'
gpx_table = 'track_points_2015'
#gpx_table = 'trackpoints_utrecht_2015'


run_types = [0]
bike_types = [1, 2 , 3]
walk_types = [14, 18]

def update_all_pixels(records, linux_mode, test_mode):
    
    try:
        cur_update, conn_update = uf.connect(linux_mode, test_mode)
        #cur_update.executemany("UPDATE " + gpx_table + " SET np_total = %s, np_run = %s, np_bike = %s, np_walk = %s where id = %s;",  
        #records)
        cur_update.executemany("Insert into " + grid_table + "(np_total, np_run, np_bike, np_walk, id, row, col, center) values (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 28992));",  
        records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        # closing database connection.
        if (conn_update):
            cur_update.close()
            conn_update.close()

def create_gpx_table(linux_mode, test_mode):

    cur1, conn1 = uf.connect(linux_mode, test_mode)
    # drop column if exist and then create route column 
    
    cur1.execute("""drop TABLE if exists """+ grid_table +""";""")
    
    
    cur1.execute("""CREATE TABLE """ + grid_table +"""(
      id integer primary key,
      row integer, col integer);""")
    
    ## geometry of the source ojbect
    cur1.execute("""
    ALTER TABLE """ + grid_table +""" DROP COLUMN IF EXISTS center;
    ALTER TABLE """ + grid_table +""" ADD COLUMN center geometry(Point,28992);""")

    conn1.commit()
    cur1.close()
    conn1.close()




def add_gpx_columns(linux_mode, test_mode):
    
    cur1, conn1 = uf.connect(linux_mode, test_mode)
     

    cur1.execute("""
        ALTER TABLE """ + grid_table +""" DROP COLUMN IF EXISTS np_total;
        ALTER TABLE """ + grid_table +""" ADD COLUMN np_total integer;
        ALTER TABLE """ + grid_table +""" DROP COLUMN IF EXISTS np_run;
        ALTER TABLE """ + grid_table +""" ADD COLUMN np_run integer;
        ALTER TABLE """ + grid_table +""" DROP COLUMN IF EXISTS np_bike;
        ALTER TABLE """ + grid_table +""" ADD COLUMN np_bike integer;
        ALTER TABLE """ + grid_table +""" DROP COLUMN IF EXISTS np_walk;
        ALTER TABLE """ + grid_table +""" ADD COLUMN np_walk integer;""")
    
    conn1.commit()
    cur1.close()
    conn1.close()

def count_gpx(geom_txt, linux_mode, test_mode, i, j):
    
    
    cur2, conn2 = uf.connect(linux_mode, test_mode)
    cur2.execute(""" SELECT sport_type 
     FROM """+ gpx_table+""" WHERE ST_Intersects(ST_GeomFromText(%s, 28992), ST_Transform(geom, 28992)) and sport_type in (0, 1, 2, 3 , 14, 18)
     """, ([geom_txt]))
    
    result_set2 = cur2.fetchall()
    if  cur2.rowcount==0:
        # print ('pixel row = ', i, 'column = ', j, ' is not overlapped with any gpx tracks')
        return 0, 0, 0, 0
    

    np_run = 0
    np_bike = 0
    np_walk = 0
    np_total = cur2.rowcount
    
    for record in result_set2:

        if record[0] in run_types: #  running: 0
            np_run = np_run + 1         
        elif record[0] in bike_types: # bike: 1 2 3
            np_bike = np_bike + 1
        elif record[0] in walk_types:  # walk: 14 18 
            np_walk = np_walk + 1
        else:
            print ("something wrong to get gpx...")
            
    return np_total, np_run, np_bike, np_walk


# create gpx tables 
create_gpx_table(linux_mode, test_mode)
add_gpx_columns(linux_mode, test_mode)

mask_array, refXSize, refYSize, refGeoTransform = uf.read_mask()
print('mask_array shape = ', mask_array.shape)
# refYsize == rows, refX == columns

pixel_index = 1
start_time =time.time()
total_start_time =time.time()
result_all_list = []

 
for i in range(refYSize):  # row
    for j in range(refXSize): #column
                
        center_x = refGeoTransform[0] + j * refGeoTransform[1] + i * refGeoTransform[2] + 0.5 * refGeoTransform[1] + 0.5 * refGeoTransform[2]
        center_y = refGeoTransform[3] + j * refGeoTransform[4] + i * refGeoTransform[5] +  + 0.5 * refGeoTransform[4] + 0.5 * refGeoTransform[5]
        if mask_array[i, j] == 0: # pixel in the land
            
            pixel_index = pixel_index + 1
            
            pixel_ring = ogr.Geometry(ogr.wkbLinearRing)
            pixel_ring.AddPoint_2D(center_x - pixel_size/2, center_y - pixel_size/2)
            pixel_ring.AddPoint_2D(center_x + pixel_size/2, center_y - pixel_size/2)
            pixel_ring.AddPoint_2D(center_x + pixel_size/2, center_y + pixel_size/2)
            pixel_ring.AddPoint_2D(center_x - pixel_size/2, center_y + pixel_size/2)
            pixel_ring.AddPoint_2D(center_x - pixel_size/2, center_y - pixel_size/2)
            pixel_poly = ogr.Geometry(ogr.wkbPolygon)
            pixel_poly.AddGeometry(pixel_ring)
            #print(i,j, pixel_poly.ExportToWkt())
            
            np_total, np_run, np_bike, np_walk = count_gpx(pixel_poly.ExportToWkt(), linux_mode, test_mode, i, j )
            result_one_tuple = (np_total, np_run, np_bike, np_walk, pixel_index, i, j, center_x, center_y)
            result_all_list.append(result_one_tuple)
            
            if np_total >0:
                print('has gpx ', result_one_tuple, i, j, pixel_index)

        if pixel_index % batch_number == 0:
            
            update_all_pixels(result_all_list, linux_mode, test_mode)
            
            end_time = time.time()
            time_diff = end_time - start_time # in seconds
            time_diff = round(time_diff, 2)
            time_diff_min = round(time_diff/60, 2) # in mins
            
            print("count gpx for grid pixel_index = " , pixel_index, "running time = ", time_diff_min, " mins! ", time_diff, " sec! ")
            logger.info("count gpx for grid pixel_index = " + str(pixel_index) + "; running time = " +  str(time_diff_min) + " mins! " + str(time_diff))
        
            # empty list
            start_time =time.time()
            result_all_list = []
                
                          
total_end_time = time.time()
total_time_diff = total_end_time - total_start_time # in seconds
total_time_diff = round(total_time_diff, 2)
total_time_diff_min = round(total_time_diff/60, 2) # in mins
print("finish counting gpx for grid pixel_index = " , pixel_index, "running time = ", total_time_diff_min, " mins! ", total_time_diff, " sec! ")
logger.info("finish counting gpx for grid  pixel_index = " + str(pixel_index) + "; running time = " +  str(total_time_diff_min) + " mins! " + str(total_time_diff))
