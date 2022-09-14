import sys
import pandas as pd
import re
import anonymizer as anon
import extract as log
import loader
import argparse
from datetime import datetime

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
parser.add_argument('-l', '--level', type=str, metavar='', required=True,choices=['bronze','silver','ouro'], help='Level of ETL (bronze, silver, gold)')
args=parser.parse_args()


# function to extract xlsx files
def extract_xlsx(path, cols: any, key, level):
    df = pd.read_excel(path, usecols=cols)
    
    if level == 'silver':
        if key == 'monthly_pay':
            anon.anonymize_name(df, 1)
        elif key == 'transfer':
            anon.anonymize_name(df, 2)

    log.log_everything(df, level)
    
    return df


# function to generate json files
def generate_json(df, level):
    df_list = list(columns.keys())
    index = 0
    for key in columns.keys():
        file = 'json/silver/{}.json'.format(df_list[index])
        df = extract_xlsx(pathing[key], columns[key], key, level)
        
        df.to_json(file, orient='records',
                   date_format='iso', force_ascii=False,)
        index += 1


def main():
    start_time = datetime.now()
    df = ''
    level = args.level
    sys.stdout.write('Executing {} procedure...\n'.format(level))
    generate_json(df, level)
    loader.run(level)
    end_time = datetime.now()

    sys.stdout.write('--- duration: {} ---\n'.format(end_time-start_time))


if __name__ == '__main__':
    main()
