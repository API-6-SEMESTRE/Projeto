import logging
from datetime import date

log_name = str(date.today())
log_name = 'logs/anonymizer'+log_name+'.log'
root_logger= logging.getLogger()
root_logger.setLevel(logging.DEBUG) # or whatever
handler = logging.FileHandler(log_name, 'a', 'utf-8') # or whatever
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')) # or whatever
root_logger.addHandler(handler)


COLUMNS = {
    'header': ['_idFile', 'contrato', 'dt_competencia'],
    'monthly_pay': ['_idFile', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],
    'transfer': ['cod_contrato', 'codigo_convenio', 'codigo_plano', 'convenio', 'competencia', 'dependente', 'dt_cancelamento',
                 'dt_situacao', 'dt_suspensao', 'inicio_vigencia', 'marca_otica', 'nXmX_bXnXfXWXXrXX', 'plano', 'saude_net_orig', 'situacao']
}


def anonymize_name(df, flag):
    if flag == 1:
        for i in range(len(df['nXmX_bXnXfiWiXriX'])):
            aux = df['nXmX_bXnXfiWiXriX'][i]
            anon = "beneficiario_marca_otica_"+str(df['marca_otica'][i])
            df['nXmX_bXnXfiWiXriX'][i] = anon
            logging.info('['+ str(df['_idFile'][i]) + ']: ' + aux + ' anonimizado para: ' + str(anon))
    else:
        for i in range(len(df['nXmX_bXnXfXWXXrXX'])):
            aux = df['nXmX_bXnXfXWXXrXX'][i]
            anon = "beneficiario_marca_otica_"+str(df['marca_otica'][i])
            df['nXmX_bXnXfXWXXrXX'][i] = anon
            logging.info('['+ str(df['cod_contrato'][i]) + ']: ' + aux + ' anonimizado para: ' + str(anon))

