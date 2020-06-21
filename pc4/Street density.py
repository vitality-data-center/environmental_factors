import sys
import psycopg2
import re
import string
import logging
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logfile = 'C:\Users\PC\AppData\Roaming\JetBrains\PyCharmCE2020.1\scratches\street_den.txt'
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
sh.setLevel(logging.WARNING)

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)


def connect_str():
    conn = psycopg2.connect(database="osm", user="postgres", host="localhost", password="13274027231Zsy!", port="5432")
    cur = conn.cursor()
    return cur, conn



def update_all_str_idx(records):
    sql = """ UPDATE pc4_2017
                  SET str_den = %s
                  WHERE pc4 = %s"""
    try:
        cur_update,conn_update = connect_str()
        cur_update.executemany(sql,records)
        conn_update.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    finally:
        if conn_update:
            cur_update.close()
            conn_update.close()


def cal_str_len (pc4, pc4_area):
    sql = """ select linestring, ST_Length(st_transform(linestring, 28992)) from ways as a,
                      geom, ST_Area(st_transform(Area, 28992)) from pc4_2017 as b,
                      where ST_Contains(a.geom, b.geom) and a.pc4 = %s"""
    cur_str, conn_str = connect_str()
    cur_str.execute(sql, str_len)
    con_str.commit()
    result_set_str = cur_str.fetchall()
    if cur_str.rowcount==0:
        print ('postcode = ', pc4), 'the polygon includes no street'
        return 0,0,0,0

    str_len = 0.0


    for record in result_set_str:
        str_len = record[0]
        str_den = str_len/pc4_area
        return str_den

cur, conn = connect_str()

cur.execute("""
ALTER TABLE pc4_2017 DROP COLUMN IF EXISTS str_den;
AlTER TABLE pc4_2017 ADD COLUMN str_den double precision;
""")

conn.commit()
cur.execute("select pc4_2017, shape_area, ST_AsText(GeomFromEWKT('SRID=4326;geom')) from pc4_2017 ")

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
    geo_wgs84_text = row[2]
    str_den = cal_str_den(pc4, float(pc4_area))


    if abs(str_den) < 0.000001:
        print("errors occured when cal street density for pc4 = ", pc4, str_den)
        print("pc4 = ", pc4, "area = ", pc4_area, "str_den = ", str_den)
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
