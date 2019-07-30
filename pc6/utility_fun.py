import psycopg2

source_table = 'pc6_2017_jan'
target_table = 'environmental_factors'

batch_number = 1000
#buffer_size_list = [300, 600, 1000]
#buffer_size_list = [600, 1000]
buffer_size_list = [2000]

linux_mode = True
test_mode = False

create_tables = True
add_bldg_column = True
add_street_column = True
add_landuse_column = True
add_airpollution_column = True
add_noise_column = True



def connect():
        
    conn = None    
    if linux_mode:
        conn = psycopg2.connect(database="postgres_nl", user="postgres",host="/var/run/postgresql", password="postgres", port="5432")
    else:
        if test_mode:
            conn = psycopg2.connect(database="postgres_test", user="postgres", password="postgres", port="5433")
        else:
            conn = psycopg2.connect(database="postgres_nl", user="postgres", password="postgres", port="5433")
    
    cur = conn.cursor()
    return cur, conn