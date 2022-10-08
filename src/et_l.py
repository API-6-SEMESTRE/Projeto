from dataclasses import replace
from multiprocessing.connection import wait
import sys
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import argparse
from datetime import datetime
import uuid
import json
import logging
import time
# dictionary that gets the files paths, standard path is 'data_sources/xlsx'
pathing = {
    'header': 'data_sources/amil_header_bronze.xlsx',
    'mensalidade': 'data_sources/amil_mensalidade_bronze.xlsx',
    'repasse': 'data_sources/amil_repasse_bronze.xlsx'
}

# dictionary that gets the columns from each file by namekey, 'namefile': ['columns']
columns = {
    'header': ['_id', '_idFile', '_lineNumber', 'contrato',  'dt_competencia', 'numero_fatura'],
    
    'mensalidade': ['_id', '_idFile', '_idheader_bronze', 'dt_inclusao', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'outros', 'outros_orig', 
    'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],
    
    'repasse': ['boleto_1', 'boleto_2', 'boleto_3', 'cod_contrato', 'codigo_convenio', 'codigo_plano', 'codigo_produto', 'codigo_segurado', 
    'competencia', 'convenio', 'dependente', 'dt_cancelamento', 'dt_geracao', 'dt_nascimento', 'dt_situacao', 'dt_suspensao', 'inicio_vigencia', 
    'marca_otica', 'marca_otica_odonto', 'nXmX_bXnXfXWXXrXX', 'odonto', 'odonto_net', 'odonto_net_orig', 'odonto_net_str', 'odonto_orig', 'odonto_str', 
    'operadora', 'parcela_1', 'plano', 'saude', 'saude_net_orig', 'saude_orig', 'situacao']
}

db = "cate"
collection = "rock"

parser=argparse.ArgumentParser(description='ETL process for excel data')
parser.add_argument('-l', '--level', type=str, metavar='', required=True,choices=['bronze','silver'], help='Level of ETL (bronze, silver)')
group = parser.add_mutually_exclusive_group()
group.add_argument('-q', '--quiet', action='store_true', help='print quiet')
group.add_argument('-v', '--verbose', action='store_true', help='print verbose')
args=parser.parse_args()


def log_everything(logger, df):
    for i in range(len(df)):
        linha_log = ""
        for j in range(len(df.max())):
            column_log = str(df.iloc[i, j])
            linha_log = linha_log + column_log + ':'
        
        if 'nan' in linha_log:
            logger.warning(linha_log)
        else:
            logger.info(linha_log)


def setup_logger(log_id):
    file_name='./logs/'+str(log_id)+'.log'
    
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
    if args.verbose:
        logging.getLogger('').addHandler(console)


def connect(user="cate", passw="api6SEM."):
    client = MongoClient(f"mongodb+srv://{user}:{passw}@cate.rem7mj8.mongodb.net/?retryWrites=true&w=majority",
                         server_api=ServerApi('1'))
    return client


def check_key_fields(df, path, logger):
    logger = logging.getLogger(args.level+'.not_found')

    if 'header' in path:
        n = len(df['contrato'])
        i = 0
        flag = False
        while i < n:
            if df['contrato'][i] == 'nan':
                s = 'ROW REMOVED '
                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n-=1
                flag = True
            if not flag:
                i+=1
            else:
                flag = False
                df = df.reset_index(drop=True)

    elif 'mensalidade' in path:
        
        n = len(df['marca_otica'])
        i = 0
        flag = False
        while i < n:
            print(df['marca_otica'])
            s = df['marca_otica'][i] == 'nan'
            s = df['valor_orig'][i] == 'nan'
            if df['marca_otica'][i] == 'nan' or df['valor_orig'][i] == 'nan':
                s = 'ROW REMOVED '
                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n-=1
                flag = True
            if not flag:
                i+=1
            else:
                flag = False
                df = df.reset_index(drop=True)
                print(df['marca_otica'])

    elif 'repasse' in path:
        n = len(df['marca_otica'])
        i = 0
        flag = False
        while i < n:
            if df['marca_otica'][i] == 'nan' or df['competencia'][i] == 'nan' or df['saude_net_orig'][i] == 'nan':
                s = 'ROW REMOVED '
                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n-=1
                flag = True
            if not flag:
                i+=1
            else:
                flag = False
                df = df.reset_index(drop=True)
    return df


# function to extract xlsx files
def extract_xlsx(path, cols: any, log_id):
    
    logger = logging.getLogger(args.level+'.extracting')

    df = pd.read_excel(path, usecols=cols)
    
    df['_idLog'] = ''
    df = df.assign(_idLog=log_id).astype('str')
    df['time_stamp'] = ''
    df = df.assign(time_stamp=datetime.now()).astype('str')
    log_everything(logger, df)

    df = check_key_fields(df, path, logger)

    return df


def mongo_find_all(user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)

    db = client[db]
    collection = db[col]
    search = collection.find()
    data = []

    for i in search:
        data.append(i)
    client.close()
    return data


def generate_json(df, log_id):
    df_list = list(columns.keys())
    index = 0
    for key in columns.keys():
        file = 'json/'+args.level+'/{}.json'.format(df_list[index])
        df = extract_xlsx(pathing[key], columns[key], log_id)
        df.to_json(file, orient='records',
                   date_format='iso', force_ascii=False,)
        index += 1
    
    return df


def mongo_insert_many(list, user="", passw="", db=db, col=collection, data_frame=None):
    
    logger = logging.getLogger(args.level+'.inserting')

    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)

    log_everything(logger, data_frame)


def run_loader(df):
    with open('json/'+args.level+'/header.json', encoding='utf-8') as file_header:
        header = json.load(file_header)

    with open('json/'+args.level+'/monthly_pay.json', encoding='utf-8') as file_monthly:
        monthly_pay = json.load(file_monthly)

    with open('json/'+args.level+'/transfer.json', encoding='utf-8') as file_transfer:
        transfer = json.load(file_transfer)

    # insert data in MongoDB collections
    mongo_insert_many(list=header, col="header_"+(args.level), data_frame=df)
    mongo_insert_many(list=monthly_pay, col="mensalidade_"+(args.level), data_frame=df)
    mongo_insert_many(list=transfer, col="repasse_"+(args.level), data_frame=df)


def main():
    print("""\
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⠤⠶⣷⠲⠤⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⠞⢉⠀⠀⠀⠿⠦⠤⢦⣍⠲⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⡤⣤⡞⢡⡶⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⢀⣤⠴⠒⣾⠿⢟⠛⠻⣿⡿⣭⠿⠁⢰⠰⠀⠀⠀⠄⣄⣀⡀⠀⠀⠘⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀
⢰⣿⣿⣦⡀⠙⠛⠋⠀⠀⠉⠻⠿⢷⣦⣿⣤⣤⣤⣤⣀⣈⠉⠛⠽⣆⡒⣿⣯⣷⣄⠀⠀⠀⠀⠀⠀
⠀⠻⣍⠻⠿⣿⣦⣄⡀⢠⣾⠑⡆⠀⠈⠉⠛⠛⢿⡿⠿⠿⢿⣿⣿⣿⣿⠟⠉⠉⢿⣟⢲⢦⣀⠀⠀
⠀⠀⠈⠙⠲⢤⣈⠉⠛⠷⢿⣏⣀⡀⠀⠀⠀⢰⣏⣳⠀⠀⠀⠀⠀⣸⣓⣦⠀⠀⠈⠛⠟⠃⣈⣷⡀
⠀⠀⠀⠀⠀⠈⢿⣙⡓⣶⣤⣤⣀⡀⠀⠀⠀⠈⠛⠁⠀⠀⠀⠀⠀⠹⣿⣯⣤⣶⣶⣶⣿⠘⡿⢸⡿
⠀⠀⠀⠀⠀⠀⠀⠙⠻⣿⡛⠻⢿⣯⣽⣷⣶⣶⣤⣤⣤⣤⣄⣀⣀⢀⣀⢀⣀⣈⣥⡤⠶⠗⠛⠋⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠓⠲⣬⣍⣉⡉⠙⠛⠛⠛⠉⠙⠉⠙⠉⣹⣿⠿⠛⠁⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠃⠀⠀⠉⠉⠉⠻⡗⠒⠒⠚⠋⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡴⠋⠀⠀⠀⠀⠀⠀⠀⠀⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⡴⠋⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣠⠞⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣠⠞⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠀⠀⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢀⠔⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠔⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀        
███████╗████████╗     ██╗     
██╔════╝╚══██╔══╝     ██║     
█████╗     ██║        ██║     
██╔══╝     ██║        ██║     
███████╗   ██║███████╗███████╗
╚══════╝   ╚═╝╚══════╝╚══════╝       
                                                                                                             
        """)
    #time.sleep(3)
    start_time = datetime.now()
    start_time_file = str(start_time).replace(':', '-')
    start_time_file = str(start_time_file).replace(' ', '_')
    
    df = ''
    
    log_id = uuid.uuid1()
    log_id = str(start_time_file) + '_' + str(log_id)
    setup_logger(log_id)

    sys.stdout.write('Executing {} procedure...\n'.format(args.level))
    sys.stdout.write('Generating json\n')
    df = generate_json(df,log_id)
    sys.stdout.write('Inserting to database...\n')
    run_loader(df)
    end_time = datetime.now()


    sys.stdout.write('--- duration: {} ---\n'.format(end_time-start_time))


if __name__ == '__main__':
    main()
