import pandas as pd
import numpy as np
headerPath = 'data_sources/amil_header_bronze.xlsx'
monthlyPayPath = 'data_sources/amil_mensalidade_bronze.xlsx'
transferPath = 'data_sources/amil_repasse_bronze.xlsx'

pathing = {
    'header': 'data_sources/amil_header_bronze.xlsx',
    'monthlyPay': 'data_sources/amil_mensalidade_bronze.xlsx',
    'transfer': 'data_sources/amil_repasse_bronze.xlsx'
}

columns = {
    'header': ['_idFile', 'contrato', 'dt_competencia'],
    'monthlyPay': ['_idFile', 'marca_otica', 'tp_beneficiario', 'valor_orig'],
    'transfer': ['cod_contrato', 'competencia', 'dependente',
                 'dt_cancelamento', 'dt_situacao', 'marca_otica', 'saude_net_orig', 'situacao']
}


def extractXlsx(path, cols: any):
    return pd.read_excel(path, usecols=cols)


dfList = list(columns.keys())
index = 0

for key in columns.keys():
    file = 'json/{}.json'.format(dfList[index])
    df = extractXlsx(pathing[key], columns[key])
    df.to_json(file, orient='records')
    index += 1
