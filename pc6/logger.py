import logging
import utility_fun as uf


def init_logger():
    logger = logging.getLogger("vdc_logger")
    logger.setLevel(logging.INFO)
    if uf.linux_mode:
        logfile = '/home/neil/log/logger.txt'
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
