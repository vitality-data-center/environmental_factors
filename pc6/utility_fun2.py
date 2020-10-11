#-------------------------------------------------------------------------------
# Name:     utilify_fun2
# Purpose:  utility function
#
# Author:      Zhiyong Wang
#
# Created:     09/2019
# Copyright:   (c) Zhiyong 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import logging
import psycopg2

source_table = 'pc6_2017_jan'
target_table = 'environmental_factors'
linux_mode = True
test_mode = False


batch_number = 1000
buffer_size_list = [300, 600, 1000]
#buffer_size_list = [600, 1000]



create_tables = False
add_bldg_column = False
add_street_column = False
add_landuse_column = False
add_airpollution_column = False
add_noise_column = False
add_ndvi_column = True
add_crossing_column = False
add_commercial_column = False
add_addr_column = False
add_no2_column = False
add_rdvi_column = False



def init_logger():
    logger = logging.getLogger("vdc_logger")
    logger.setLevel(logging.INFO)
    if linux_mode:
        logfile = '/home/neil/log/logger.txt'
        #logfile = '/home/zywang/python/log/logger.txt'
    else:
        logfile = 'J://vdc_workspace//vdc//log//logger.txt'
    
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)   
    
    
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
        
    logger.addHandler(fh) 
    logger.addHandler(ch)




def connect():
        
    conn = None    
    if linux_mode:
        #conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
        conn = psycopg2.connect(database="postgres_nl", user="zywang",host="/tmp/", password="2203930_ZyW", port="5432")
    else:
        if test_mode:
            conn = psycopg2.connect(database="postgres_test", user="postgres", password="postgres", port="5433")
        else:
            conn = psycopg2.connect(database="postgres_nl", user="postgres", password="postgres", port="5433")
    
    cur = conn.cursor()
    return cur, conn
