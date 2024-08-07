### pip install gpxpy
# https://gist.github.com/rcoh/ffe530cac090a3e87f3578a399e98889

# about install gdal, import problem
# better use whl instead of conda
# https://blog.csdn.net/ivan_ljf/article/details/77005083

# https://gis.stackexchange.com/questions/46893/getting-pixel-value-of-gdal-raster-under-ogr-point-without-numpy

import psycopg2
import sys
import json
import datetime
import gdal,ogr
import os
import datetime
import logging
 

logger = logging.getLogger()
logger.setLevel(logging.INFO)   
 
logfile = '/home/neil/log/logger.txt'
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)   
 
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


    


# global
point_id = 0;

def create_tables():
    
    cur, conn = connect()
    
    # drop column if exist and then create route column 

    cur.execute("""
    drop table if exists tracks_2015;
CREATE TABLE tracks_2015 (
    workout_id BIGINT PRIMARY KEY,
    author_id integer,
    sport_type integer,
    speed_max double precision,
    speed_avg double precision,
    distance double PRECISION, 
    duration double precision,
    start_time TIMESTAMP,
    local_start_time TIMESTAMP,
    ascent double precision, 
    dscent double precision, 
    weather_type integer,
    hydration_rate double PRECISION,
    calories integer, 
    geom geometry(linestring, 4326)
);

drop table if exists track_points_2015;
CREATE TABLE track_points_2015(
    id BIGINT PRIMARY KEY,
    workout_id bigint, 
    speed double precision,
    altitude double precision,
    distance double precision, 
    duration double precision,
    time TIMESTAMP,
    instruction integer,
    geom geometry(point, 4326)
);""")
    
    
    conn.commit()
    cur.close()
    conn.close()
    

def connect():
    conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
    cur = conn.cursor()
    return cur, conn


def insert_track_point(id, workout_id, speed, altitude, distance, duration, instruction, lon, lat, date_time):
    cur_point, conn_point = connect()
    cur_point.execute("INSERT INTO track_points_2015 (id, workout_id, speed, altitude, distance, duration, instruction, time, geom) values (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));",  
        ([id, workout_id, speed, altitude, distance, duration, instruction, date_time, lon, lat]))
     
    conn_point.commit()
    cur_point.close()
    conn_point.close()
    
def insert_multi_track_point(records):
    try:
        cur_point, conn_point = connect()
        cur_point.executemany("INSERT INTO track_points_2015 (id, workout_id, speed, altitude, distance, duration, instruction, time, geom) values (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));",  
        records)
        conn_point.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))
    
    finally:
        # closing database connection.
        if (conn_point):
            cur_point.close()
            conn_point.close()

        
def exists_workout_id(workout_id): 
    cur_exists, conn_exists = connect()
    cur_exists.execute("select exists(select 1 from tracks_2015 where workout_id = %s)",
               ([workout_id]))
    record_result_set = cur_exists.fetchall()
    results = record_result_set[0]
    if_exists = results[0]
    cur_exists.close()
    conn_exists.close()
    return if_exists
    


def convert(f):
    
    global point_id
    
    with open(f, 'r') as file:
        index_track = 0 
        for line in file:
            index_track = index_track + 1
            track_data = None
            try:
                track_data = json.loads(line)
                workout_id =  track_data["workout_id"]
                author_id  =  track_data["author_id"]
                sport_type =  track_data.get("sport_type")
                speed_max  =  track_data.get("speed_max")
                speed_avg  =  track_data.get("speed_avg")
                distance   =  track_data.get("distance")     
                duration   =  track_data.get("duration")
                start_time =  track_data.get("start_time")
                local_start_time  =  track_data.get("local_start_time")
                ascent     =  track_data.get("ascent")
                dscent     =  track_data.get("descent")
                weather_type =  track_data.get("weather_type")      
                hydration_rate =  track_data.get("hydration_rate")
                calories   =  track_data.get("calories")
            except (KeyError, ValueError) as e:
                print('track error', line)
                print('track exception', e.args)
                
            if exists_workout_id(workout_id):
                #print "workout_id exists", workout_id
                continue  
            #else:
                #print "new workout_id ***********", workout_id     

            track_point_array = track_data.get("track_points")
            
            linestring = None
            if track_point_array is not None:
                track_point_list=[]
                linestring = ogr.Geometry(ogr.wkbLineString)
                for track_point in track_point_array:  # longitude, latitude, altitude, time, speed
                    
                    track_point_tuple=()
                    
                    point_speed     = track_point.get('speed')
                    point_altitude  = track_point.get("altitude")
                    point_distance  =  track_point.get("distance")     
                    point_duration  =  track_point.get("duration")
                    point_instruction  =  track_point.get("instruction")

                    date_time = None
                    try:
                                
                        lon  = track_point["longitude"]
                        lat  =  track_point['latitude'] 
                        
                        date_format = "%Y-%m-%dT%H:%M:%S.%fZ" 
                        #time =  track_point.get('time', datetime.datetime(2015, 1, 12, 23, 9, 12, 946118).strftime(date_format))                   
                        #date_time = dateutil.parser.parse(time) # datetime.datetime.fromtimestamp(time)
                        date_time =  track_point.get('time')
                        if date_time is not None:
                            date_time = datetime.datetime.strptime(date_time, date_format)
                    except (AttributeError, ValueError, UnboundLocalError) as e:
                        print('track point error',  track_point)
                        print("exception =  ", e.args)
                        date_time = ""
                
                    point_id =  point_id + 1
                    linestring.AddPoint_2D(lon, lat)
                    # insert point
                    #insert_track_point(point_id, workout_id, point_speed, point_altitude, point_distance, point_duration, point_instruction, lon, lat, date_time)
                    track_point_tuple = (point_id, workout_id, point_speed, point_altitude, point_distance, point_duration, point_instruction, date_time, lon, lat)
                    track_point_list.append(track_point_tuple)

                # insert mutiple track points if not null
                insert_multi_track_point(track_point_list)
                # print linestring.ExportToWkt()  # end line is not null  
            
            # insert track
            cur_track, conn_track = connect()
            
            if linestring is not None and  linestring.GetPointCount() > 1 :
                geom_text = linestring.ExportToWkt() 
                cur_track.execute("INSERT INTO tracks_2015 (workout_id, author_id, sport_type, speed_max, speed_avg, distance, duration, start_time, local_start_time, ascent, dscent, weather_type, hydration_rate, calories, geom) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_GeomFromText(%s, 4326));",  
                              ([workout_id, author_id, sport_type, speed_max, speed_avg, distance, duration, start_time, local_start_time, ascent, dscent, weather_type, hydration_rate, calories, geom_text]))
            else:
                cur_track.execute("INSERT INTO tracks_2015 (workout_id, author_id, sport_type, speed_max, speed_avg, distance, duration, start_time, local_start_time, ascent, dscent, weather_type, hydration_rate, calories) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",  
                              ([workout_id, author_id, sport_type, speed_max, speed_avg, distance, duration, start_time, local_start_time, ascent, dscent, weather_type, hydration_rate, calories]))
            #cur_track.execute("INSERT INTO tracks_2015 (workout_id) values (%s);", 
            #                  ([workout_id]))
            conn_track.commit()
            cur_track.close()
            conn_track.close()
            
            if index_track % 100==0:
                print("has processed tracks ", index_track)
                logging.info('has processed tracks ' + str(index_track))
                
            


    print("total track number = ", index_track)
    logging.info('totaol track number = ' + str(index_track))
    print("Done!")
        
        
        
        
if __name__ == "__main__":
    
    create_tables()
    #starttime =datetime.datetime.now()
    #convert("C://gpx_2015//NLTrackPart5.json")
    #endtime =datetime.datetime.now()
    #print "runing time = ", (endtime - starttime).seconds
    for root, dirs, files in os.walk("/home/neil/data/gpx_2015/"):
        for file in files:
            file_path = os.path.join(root, file)
            starttime =datetime.datetime.now()
            convert(file_path)
            endtime =datetime.datetime.now()
            logging.info('has processed file ' + str(file_path))
            logging.info("running time = " + str((endtime - starttime).seconds/3600) + " hours!")
            print "running time for file = ", file_path, " = ", (endtime - starttime).seconds/3600, " hours!"
            #print "exists workout_id", exists_workout_id(594098206)
            #if exists_workout_id(594098206) is False:
            #print "has processed file ", file_path