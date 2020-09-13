import utility_fun2 as uf
import time
import logging



logger = logging.getLogger("vdc_logger")




def add_columns(buffer):
    
    cur1, conn1 = uf.connect()
     
    ## residential building area and density
    if uf.add_resi_column:
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
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS landuse_idx5;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN landuse_idx5 double precision;""")
        
        
    # ndvi index 
    if uf.add_ndvi_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS ndvi_avg;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN ndvi_avg double precision;""") 
        
    # crossing index 
    if uf.add_crossing_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS crossing_num;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN crossing_num double precision;
        """)
    if uf.add_crossing_column2:        
        cur1.execute("""
        ALTER TABLE """ +uf.target_table + """ DROP COLUMN IF EXISTS crossing1""" +'_'+str(buffer)+""";
        ALTER TABLE """ +uf.target_table + """ ADD COLUMN crossing1""" +'_'+ str(buffer)+ """ double precision;
        ALTER TABLE """ +uf.target_table + """ DROP COLUMN IF EXISTS crossing3""" +'_'+str(buffer)+""";
        ALTER TABLE """ +uf.target_table + """ ADD COLUMN crossing3""" +'_'+ str(buffer)+ """ double precision;
        ALTER TABLE """ +uf.target_table + """ DROP COLUMN IF EXISTS crossing4plus""" +'_'+str(buffer)+""";
        ALTER TABLE """ +uf.target_table + """ ADD COLUMN crossing4plus""" +'_'+ str(buffer)+ """ double precision;""")       
    
    if uf.add_bldg_column:
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


    # rdvi index # remove zeros
    if uf.add_green_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table + """ DROP COLUMN IF EXISTS green""" +'_'+str(buffer)+""";
        ALTER TABLE """ +uf.target_table + """ ADD COLUMN green""" +'_'+ str(buffer)+ """ double precision;""")       
        
        
    # blue index # remove zeros
    if uf.add_blue_column:
        cur1.execute("""
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" DROP COLUMN IF EXISTS ndvi_blue;
        ALTER TABLE """ +uf.target_table+'_'+str(buffer)+""" ADD COLUMN ndvi_blue double precision;""")             
                          

    conn1.commit()
    cur1.close()
    conn1.close()
    
    return uf.target_table+'_'+str(buffer)