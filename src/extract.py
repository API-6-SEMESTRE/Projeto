import logging
from datetime import date

def log_everything(df, level):
    log_name = str(date.today())
    
    if level == 'bronze':
        log_name = 'logs/bronze_'+log_name+'.log'
    elif level == 'silver':
        log_name = 'logs/silver_'+log_name+'.log'
    
    root_logger= logging.getLogger()
    root_logger.setLevel(logging.DEBUG) 
    handler = logging.FileHandler(log_name, 'a', 'utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')) 
    root_logger.addHandler(handler)
    
    
    
    for i in range(len(df)):
        linha_log = ""
        for j in range(len(df.max())):
            column_log = str(df.iloc[i,j])
            linha_log = linha_log + column_log + ':'
        logging.info(linha_log)
    
        

