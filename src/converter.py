import pandas as pd

# dictionary that gets the files paths, standard path is 'data_sources/xlsx'
pathing = {
    'header': 'data_sources/amil_header_bronze.xlsx',
    'monthlyPay': 'data_sources/amil_mensalidade_bronze.xlsx',
    'transfer': 'data_sources/amil_repasse_bronze.xlsx'
}

# dictionary that gets the columns from each file by namekey, 'namefile': ['columns']
columns = {
    'header': ['_idFile', 'contrato', 'dt_competencia'],
    'monthlyPay': ['_idFile', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],
    'transfer': ['cod_contrato', 'codigo_convenio', 'codigo_plano', 'convenio', 'competencia', 'dependente', 'dt_cancelamento',
                 'dt_situacao', 'dt_suspensao', 'inicio_vigencia', 'marca_otica', 'nXmX_bXnXfXWXXrXX', 'plano', 'saude_net_orig', 'situacao']
}


# function to extract xlsx files
def extractXlsx(path, cols: any):
    return pd.read_excel(path, usecols=cols)


# set parameters for looping
dfList = list(columns.keys())
index = 0


# looping the process of extraction and generate the json files, the standard path is json/file.json
for key in columns.keys():
    file = 'json/{}.json'.format(dfList[index])
    df = extractXlsx(pathing[key], columns[key])
    df.to_json(file, orient='records')
    index += 1
