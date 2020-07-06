import sys
from typing import Tuple
import psycopg2
import math
import string
import logging
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logfile = 'C:/Windows/Temp/logger.txt'
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setLevel(logging.WARNING)

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)


def connect():
    conn = psycopg2.connect(database="osm", user="postgres", host="localhost", password="13274027231Zsy", port="5432")
    cur = conn.cursor()
    return cur, conn



def update_all_str_idx(records):
    sql = """ UPDATE pc4_2017
                  SET str_den = %s
                  WHERE pc4 = %s"""
    try:
        cur_update,conn_update = connect()
        cur_update.executemany(sql,records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        if conn_update:
            cur_update.close()
            conn_update.close()


def cal_str_den(pc4,pc4_area):

 #   sql = """ select ST_Length(b3.geom) from public.pc4_2017 b1, public.reprojected b3 where pc4 = %s and ST_Intersects(b3.geom, b1.geom) and ST_IsValid(b1.geom) is true """
    cur_str, conn_str = connect()
    cur_str.execute("select ST_Length(b3.geom) from public.pc4_2017 b1, public.reprojected b3 where pc4 = %s and ST_Intersects(b3.geom, b1.geom);",([pc4]))
    result_set_str = cur_str.fetchall()


    str_len_sum = 0.0
    str_den = 0.0
    for record in result_set_str:
        str_len = result_set_str[0]

        str_len_str = str(str_len)
        change_str = str_len_str.strip(',()')
        str_len_float = float(change_str)
        str_len_sum = str_len_sum + str_len_float

    str_den = str_len_sum / pc4_area + str_den
    #check length of all streets in a pc4 area
    logging.info(f'street length in the pc4 {pc4}:'+ str(str_len_sum) + f'street density in the pc4 {pc4}:'+ str(str_den))
    if cur_str.rowcount == 0:
        print('postcode = ', pc4), 'the polygon includes no street'
        return 0, 0

    cur_str.close()
    cur_str.close()

    return str_den


cur, conn = connect()

cur.execute("""
ALTER TABLE pc4_2017 DROP COLUMN IF EXISTS str_den;
AlTER TABLE pc4_2017 ADD COLUMN str_den double precision;
""")

conn.commit()
cur.execute(" select pc4, ST_Area(geom) from public.pc4_2017")
record_result_set = cur.fetchall()

record_number = cur.rowcount
row_index = 0
str_den_list = []
starttime = datetime.datetime.now()

for row in record_result_set:
    row_index = row_index + 1

    str_den_tuple=()
    pc4 = row[0]
    pc4_area = row[1]


#   geo_wgs84_text = row[2]
    str_den = cal_str_den(pc4, float(pc4_area))


    if abs(str_den) < 0.000001:
        print("errors occured when cal street density for pc4 = ", pc4)
        print("area = ", pc4_area, "street density = ", str_den)
        logging.info('"something wrong with calc street density for pc4 = ' + str(pc4) + " ; str_den ="+ str(str_den))
        logging.info("pc4 = " + str(pc4) + "; area = " + str(pc4_area) + "; str_den = " + str(str_den))

    str_den_tuple = (str_den, pc4)
    str_den_list.append(str_den_tuple)

    if row_index % 10 == 0:
        update_all_str_idx(str_den_list)
        endtime = datetime.datetime.now()
        print("for pc4 ", pc4, row_index, "; running time = ", (endtime - starttime).seconds / 60, " mins! ", (endtime - starttime).seconds, " seconds!")
        starttime = datetime.datetime.now()
        str_den_list = []

    #keep recording the updated rows
    logging.info(f"polygon {pc4}")
    update_all_str_idx(str_den_list)

cur.close()
conn.close()
