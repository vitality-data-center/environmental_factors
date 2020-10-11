#!/usr/bin/python
#coding:utf-8
#-------------------------------------------------------------------------------
# Name:     main_fun
# Purpose:  the main function
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import init_database
# import calc_bldg_gpx
#import calc_street_p6
#import calc_noise_p6
#import calc_air_p6_v2
#import calc_landuse_gpx
#import calc_address_gpx
#import calc_blue_gpx
#import calc_green_gpx
import calc_crossing_gpx
#import calc_no2_p6
import utility_fun2 as uf
import logging
import time




def calc_index():
    
    for i in range(len(uf.buffer_size_list)):
        current_buffer = uf.buffer_size_list[i]
        init_database.add_columns(current_buffer)
    
    # crossing
    calc_crossing_gpx.calc() 
    
    # building
    # calc_bldg_gpx.calc(target_buffer_table, buffer)
    
    # address
    # calc_address_gpx.calc(target_buffer_table, buffer)
    
    # no2 in 2011
    # calc_no2_p6.calc(target_buffer_table, buffer)
    
    
    


#   
#    # calc street density   
     #start_time = time.time()
     #calc_street_p6.calc(target_buffer_table,buffer)
     #end_time = time.time()
     #time_diff = end_time - start_time # in seconds
     #time_diff = round(time_diff/(60*60), 2)   # in hours
     #print("finish calculating street index...processed time in hours = ", time_diff," buffer size = ", buffer)
     #vdc_logger.info("finish calculating street index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
#       
#       
#       
#    # calc landuse index  
#    start_time = time.time()
#    calc_landuse_gpx.calc(target_buffer_table, buffer) # landuse
#    end_time = time.time()
#    time_diff = end_time - start_time # in seconds
#    time_diff = round(time_diff/(60*60), 2)   # in hours
#    print("finish calculating landuse index...processed time in hours = ", time_diff," buffer size = ", buffer)
#    vdc_logger.info("finish calculating landuse index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
#       
#     # calc noise index  
#     start_time = time.time()
#     #calc_noise_p6.calc(target_buffer_table, buffer)
#     end_time = time.time()
#     time_diff = end_time - start_time # in seconds
#     time_diff = round(time_diff/(60*60), 2)   # in hours
#     print "finish calculating noise index...processed time in hours = ", time_diff," buffer size = ", buffer
#     vdc_logger.info("finish calculating noise index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
#   
#   
#   
    #calc air pollution index  
    #start_time = time.time()
 #   calc_air_p6_v2.calc(target_buffer_table, buffer)
    #end_time = time.time()
    #time_diff = end_time - start_time # in seconds
    #time_diff = round(time_diff/(60*60), 2)   # in hours
    #print "finish calculating air pollution index...processed time in hours = ", time_diff," buffer size = ", buffer
    #vdc_logger.info("finish calculating air pollution index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
       
    
    
    #calc ndvi index  
    #calc_blue_gpx.calc(target_buffer_table, buffer) # blue
    #calc_green_gpx.calc() # green

    
    
    


# main function
uf.init_logger()
vdc_logger = logging.getLogger("vdc_logger")
print("start calculating index ... ")
calc_index()

