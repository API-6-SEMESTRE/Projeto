import sys
import pandas as pd
import anonymizer as anon
import log as log
import loader
import argparse
from datetime import datetime
import uuid

# dictionary that gets the files paths, standard path is 'data_sources/xlsx'
pathing = {
    'header': 'data_sources/amil_header_bronze.xlsx',
    'monthly_pay': 'data_sources/amil_mensalidade_bronze.xlsx',
    'transfer': 'data_sources/amil_repasse_bronze.xlsx'
}

# dictionary that gets the columns from each file by namekey, 'namefile': ['columns']
columns = {
    'header': ['_idFile', 'contrato', 'dt_competencia'],
    'monthly_pay': ['_idFile', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],
    'transfer': ['cod_contrato', 'codigo_convenio', 'codigo_plano', 'convenio', 'competencia', 'dependente', 'dt_cancelamento',
                 'dt_situacao', 'dt_suspensao', 'inicio_vigencia', 'marca_otica', 'nXmX_bXnXfXWXXrXX', 'plano', 'saude_net_orig', 'situacao']
}

parser=argparse.ArgumentParser(description='ETL process for excel data')
parser.add_argument('-l', '--level', type=str, metavar='', required=True,choices=['bronze','silver','gold'], help='Level of ETL (bronze, silver, gold)')
args=parser.parse_args()

# function to extract xlsx files
def extract_xlsx(path, cols: any, key, level, log_id):
    
    logger=log.setup_logger(level, 'extracting', log_id)

    df = pd.read_excel(path, usecols=cols)
    
    df['_idLog'] = ''
    df = df.assign(_idLog=log_id).astype('str')

    log.log_everything(logger, df)

    if level == 'silver':
        if key == 'monthly_pay':
            anon.anonymize_name(df, 1, level, log_id)
        elif key == 'transfer':
            anon.anonymize_name(df, 2, level, log_id)
    
    return df


# function to generate json files
def generate_json(df, level, log_id):
    df_list = list(columns.keys())
    index = 0
    for key in columns.keys():
        file = 'json/silver/{}.json'.format(df_list[index])
        df = extract_xlsx(pathing[key], columns[key], key, level, log_id)
        
        df.to_json(file, orient='records',
                   date_format='iso', force_ascii=False,)
        index += 1
    return df


def main():
    start_time = datetime.now()
    df = ''
    level = args.level
    log_id = uuid.uuid1()
    sys.stdout.write('Executing {} procedure...\n'.format(level))
    sys.stdout.write('Generating json\n')
    df = generate_json(df,level,log_id)
    loader.run(df,level,log_id)
    end_time = datetime.now()
    
    sys.stdout.write('--- log id: {} ---\n'.format(log_id))
    sys.stdout.write('--- duration: {} ---\n'.format(end_time-start_time))


if __name__ == '__main__':
    main()
