import logging
from datetime import date
import pandas as pd
import log

pd.options.mode.chained_assignment = None  # default='warn'


COLUMNS = {
    'header': ['_idFile', 'contrato', 'dt_competencia'],
    'monthly_pay': ['_idFile', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],
    'transfer': ['cod_contrato', 'codigo_convenio', 'codigo_plano', 'convenio', 'competencia', 'dependente', 'dt_cancelamento',
                 'dt_situacao', 'dt_suspensao', 'inicio_vigencia', 'marca_otica', 'nXmX_bXnXfXWXXrXX', 'plano', 'saude_net_orig', 'situacao']
}

PROCESS_NAME='anonymizing'

def anonymize_name(df, flag, level, log_id):
    logger = log.setup_logger(level, PROCESS_NAME, log_id)
    if flag == 1:
        for i in range(len(df['nXmX_bXnXfiWiXriX'])):
            aux = df['nXmX_bXnXfiWiXriX'][i]
            anon = "beneficiario_marca_otica_"+str(df['marca_otica'][i])
            
            if not df['marca_otica'][í]:
                logger.error(str(df['_idFile'][i]) + ': ' + aux + ' anonimização falhou! Linha não possui marca_otica')
                continue
                
            df['nXmX_bXnXfiWiXriX'][i] = anon
            
            if 'nan' in aux:
                logger.warning(str(df['_idFile'][i]) + ': ' + aux + ' anonimizado para: ' + str(anon))
            else:
                logger.info(str(df['_idFile'][i]) + ': ' + aux + ' anonimizado para: ' + str(anon))
            
            
    else:
        for i in range(len(df['nXmX_bXnXfXWXXrXX'])):
            aux = df['nXmX_bXnXfXWXXrXX'][i]
            check_nan = df['cod_contrato'][i]
            anon = "beneficiario_marca_otica_"+str(df['marca_otica'][i])
            
            df['nXmX_bXnXfXWXXrXX'][i] = anon
            
            if 'nan' in check_nan:
                logger.warning('['+ str(df['cod_contrato'][i]) + ': ' + aux + ' anonimizado para: ' + str(anon))
            else:
                logger.info('['+ str(df['cod_contrato'][i]) + ': ' + aux + ' anonimizado para: ' + str(anon))
            
    


