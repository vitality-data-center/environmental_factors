#!/usr/bin/python
#coding:utf-8
from itertools import product
import string
import sys
import re
import math
from decimal import Decimal 
import init_database
import calc_bldg_p6
import calc_street_p6
import calc_noise_p6
import calc_air_p6
import calc_landuse_p6
import logger
import logging
import time
import utility_fun as uf



def calc_index(buffer):
    
    init_database.get_target_table(buffer)
    target_buffer_table  = init_database.add_columns(buffer)

    # calc residential density  
    start_time = time.time()
    calc_bldg_p6.calc(target_buffer_table,buffer)
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    time_diff = round(time_diff/(60*60),2)# in hours
    print "finish calculating bldg index...processed time in hours = ", time_diff, " buffer size = ", buffer
    vdc_logger.info("finish calculating bldg index...processed time in hours = "+ str(time_diff) + " buffer size = " + str(buffer))

    # calc street density  
    start_time = time.time()
    calc_street_p6.calc(target_buffer_table,buffer)
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    time_diff = round(time_diff/(60*60), 2)   # in hours
    print "finish calculating street index...processed time in hours = ", time_diff," buffer size = ", buffer
    vdc_logger.info("finish calculating street index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
    
    
    
    # calc landuse index  
    start_time = time.time()
    calc_landuse_p6.calc(target_buffer_table, buffer) # landuse
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    time_diff = round(time_diff/(60*60), 2)   # in hours
    print "finish calculating landuse index...processed time in hours = ", time_diff," buffer size = ", buffer
    vdc_logger.info("finish calculating landuse index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
    
    # calc noise index  
    start_time = time.time()
    calc_noise_p6.calc(target_buffer_table, buffer)
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    time_diff = round(time_diff/(60*60), 2)   # in hours
    print "finish calculating noise index...processed time in hours = ", time_diff," buffer size = ", buffer
    vdc_logger.info("finish calculating noise index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))



    # calc air pollution index  
    start_time = time.time()
    calc_air_p6.calc(target_buffer_table, buffer)
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    time_diff = round(time_diff/(60*60), 2)   # in hours
    print "finish calculating air pollution index...processed time in hours = ", time_diff," buffer size = ", buffer
    vdc_logger.info("finish calculating air pollution index...processed time in hours= "+ str(time_diff) + " buffer size = " + str(buffer))
    
    
    


# main function
logger.init_logger()
vdc_logger = logging.getLogger("vdc_logger")
for i in range(len(uf.buffer_size_list)):
    current_buffer = uf.buffer_size_list[i]
    print "start calculating index for buffer ", current_buffer
    vdc_logger.info("start calculating index for buffer"+ str(current_buffer))
    calc_index(current_buffer)

