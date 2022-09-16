import logging
from datetime import date
import uuid


def log_everything(logger, df):
    #log_name = str(date.today())

    #if level == 'bronze':
     #   log_name = 'logs/bronze_'+log_name+'.log'
    #elif level == 'silver':
     #   log_name = 'logs/silver_'+log_name+'.log'

    #handler = logging.FileHandler(log_name, 'a', 'utf-8')

    for i in range(len(df)):
        linha_log = ""
        for j in range(len(df.max())):
            column_log = str(df.iloc[i, j])
            linha_log = linha_log + column_log + ':'
        logger.info(linha_log)


def setup_logger(level, process_name, log_id):
    file_name='./logs/'+str(log_id)+'.log'
    # set up logging to file - see previous section for more details
    
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=file_name,
                        filemode='w',
                        encoding='utf-8'
                        )
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
        # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
    console.setFormatter(formatter)
        # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    
    logger = logging.getLogger(level+'.extractor.'+process_name)
    #logger2 = logging.getLogger('extractor.inserting')
    #logger3 = logging.getLogger('extractor.anonymizing')

    #logger1.debug('Quick zephyrs blow, vexing daft Jim.')
    #logger1.info('How quickly daft jumping zebras vex.')
    #logger2.warning('Jail zesty vixen who grabbed pay from quack.')
    #logger2.error('The five boxing wizards jump quickly.')

    return logger

    # Now, define a couple of other loggers which might represent areas in your
    # application:

