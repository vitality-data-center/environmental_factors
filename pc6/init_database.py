import utility_fun as uf
import time
import logging



logger = logging.getLogger("vdc_logger")

def get_target_table(buffer):
    
    if not uf.create_tables:
        return uf.target_table+'_'+str(buffer)
    
    
    start_time =time.time()
    
    cur1, conn1 = uf.connect()
    # drop column if exist and then create route column 
    
    cur1.execute("""drop TABLE if exists """+ uf.target_table+'_'+str(buffer)+""";""")
    
    
    cur1.execute("""CREATE TABLE """ +uf.target_table+'_'+str(buffer)+""" (
      id integer primary key,
      pc6 character(6)
     );""")
    
    ## geometry of the source ojbect
    cur1.execute("""
    ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS geom;
    ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN geom geometry(MultiPolygon,28992);""")

    cur1.execute(""" INSERT INTO """ +uf.target_table+'_'+str(buffer)+"""(id, pc6, geom) 
    (SELECT gid, pc6, geom FROM pc6_2017_jan)""");


    ## copy geometry and id
    conn1.commit()
    cur1.close()
    conn1.close()
    
    
    end_time = time.time()
    time_diff = end_time - start_time # in seconds
    print "create table for ", uf.target_table+'_'+str(buffer), "; running time = ", (time_diff/60), " mins! ", time_diff, " sec! ", time_diff*1000, " million seconds!"
    logger.info("create table for "+ uf.target_table+'_'+str(buffer) + "; running time = " + str((time_diff/60)) + " mins! " + str(time_diff) + " sec! " + str(time_diff*1000) + " million seconds!")  
    
    return uf.target_table+'_'+str(buffer)





def add_columns(buffer):
    
    cur1, conn1 = uf.connect()
     
    ## residential building area and density
    if uf.add_bldg_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS res_bldg_area;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN res_bldg_area double precision default 0;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS res_bldg_density;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN res_bldg_density double precision default 0;""")
    
   
    if uf.add_street_column:
        ## motorway length and density
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS motorway_length;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN motorway_length double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS motorway_density;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN motorway_density double precision;""")
        
        ## street length and density
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS street_length;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN street_length double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS street_density;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN street_density double precision;""")
     
      
    ## noise column
    if uf.add_noise_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_1;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_1 double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_2;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_2 double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_3;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_3 double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_4;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_4 double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_5;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_5 double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS dn_6;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN dn_6 double precision;""")
       
    ## air pollution column
    if uf.add_airpollution_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS no2_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN no2_avg double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS nox_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN nox_avg double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS pm25_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN pm25_avg double precision;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS pm10_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN pm10_avg double precision;""")   
       
    # landuse index 
    if uf.add_landuse_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS landuse_idx;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN landuse_idx double precision;""")
        
        
    # ndvi index 
    if uf.add_ndvi_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS ndvi_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN ndvi_avg double precision;""") 
        
    # crossing index 
    if uf.add_crossing_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS crossing_num;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN crossing_num double precision;""")
    
    
    if uf.add_commercial_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS comm_bldg_ratio;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN comm_bldg_ratio double precision; 
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS resi_bldg_ratio;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN resi_bldg_ratio double precision;""")
        
    # degree of urbanization 
    if uf.add_addr_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS addr_num;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN addr_num double precision;""")
    
    # no2 in 2011 
    if uf.add_no2_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS no2_2011;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN no2_2011 double precision;""")         


    # rdvi index 
    if uf.add_rdvi_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS rdvi_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN rdvi_avg double precision;""")             
                          

    conn1.commit()
    cur1.close()
    conn1.close()
    
    return uf.target_table+'_'+str(buffer)