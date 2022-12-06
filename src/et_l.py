import sys
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import argparse
from datetime import datetime
import uuid
import json
import logging
import warnings
import traceback
from mongo_portal import mongo_find_all
from datetime import datetime
import warnings
import mysql.connector
import traceback
import delete_log

warnings.filterwarnings("ignore", category=FutureWarning)

api_db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dw'
)

cursor = api_db.cursor()

pathing = {
    'header': 'data_sources/Julho_Agosto/amil_header_bronze.xlsx',
    'mensalidade': 'data_sources/Julho_Agosto/amil_mensalidade_bronze.xlsx',
    'repasse': 'data_sources/Julho_Agosto/amil_repasse_bronze.xlsx'
}

columns = {
    'header': ['_idFile', '_lineNumber', 'contrato', 'dt_competencia', 'numero_fatura'],

    'mensalidade': ['_id', '_idFile', '_idheader_bronze', 'dt_inclusao', 'marca_otica', 'nXmX_bXnXfiWiXriX', 'outros',
                    'outros_orig',
                    'plano', 'rubrica', 'tp_beneficiario', 'valor_orig'],

    'repasse': ['boleto_1', 'boleto_2', 'boleto_3', 'cod_contrato', 'codigo_convenio', 'codigo_plano', 'codigo_produto',
                'codigo_segurado',
                'competencia', 'convenio', 'dependente', 'dt_cancelamento', 'dt_geracao', 'dt_nascimento',
                'dt_situacao', 'dt_suspensao', 'inicio_vigencia',
                'marca_otica', 'marca_otica_odonto', 'nXmX_bXnXfXWXXrXX', 'odonto', 'odonto_net', 'odonto_net_orig',
                'odonto_net_str', 'odonto_orig', 'odonto_str',
                'operadora', 'parcela_1', 'plano', 'saude', 'saude_net_orig', 'saude_orig', 'situacao']
}

db = "cate"
collection = "rock"

parser = argparse.ArgumentParser(description='ETL process for excel data')
parser.add_argument('-l', '--level', type=str, metavar='', required=True,
                    choices=['bronze', 'silver', 'gold', 'all'], help='Level of ETL (bronze, silver, gold, all)')
group = parser.add_mutually_exclusive_group()
group.add_argument('-q', '--quiet', action='store_true', help='print quiet')
group.add_argument('-v', '--verbose', action='store_true',
                   help='print verbose')
args = parser.parse_args()


# BEG BRONZE
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


def save_logging(log_id):
    with open('logs/' + log_id + '.log', encoding='utf-8') as f:
        lines = []
        for _ in f:
            lines = f.readlines()

    df_log_mongo = pd.DataFrame(lines)
    records = json.loads(df_log_mongo.T.to_json()).values()
    mongo_insert_log(list=records, db="cate", col="logs")


def setup_logger(log_id):
    file_name = './logs/' + str(log_id) + '.log'

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
    logger = logging.getLogger(args.level + '.not_found')

    header_error = pd.DataFrame(columns=columns['header'])
    mensalidade_error = pd.DataFrame(columns=columns['mensalidade'])
    repasse_error = pd.DataFrame(columns=columns['repasse'])

    if 'header' in path:
        n = len(df['contrato'])
        i = 0
        flag = False
        while i < n:
            if df['contrato'][i] == 'nan':
                s = 'ROW REMOVED '
                linha = df.iloc[i]

                header_error = header_error.append(
                    linha, ignore_index=True, verify_integrity=False)

                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n -= 1
                flag = True
            if not flag:
                i += 1
            else:
                flag = False
                df = df.reset_index(drop=True)
        generate_michael(header_error, 'header_error')

    elif 'mensalidade' in path:
        n = len(df['marca_otica'])
        i = 0
        flag = False
        while i < n:
            if df['marca_otica'][i] == 'nan' or df['valor_orig'][i] == 'nan':
                linha = df.iloc[i]
                mensalidade_error = mensalidade_error.append(
                    linha, ignore_index=True, verify_integrity=False)
                s = 'ROW REMOVED '
                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n -= 1
                flag = True
            if not flag:
                i += 1
            else:
                flag = False
                df = df.reset_index(drop=True)
        generate_michael(mensalidade_error, 'mensalidade_error')

    elif 'repasse' in path:
        n = len(df['marca_otica'])
        i = 0
        flag = False
        while i < n:
            if df['marca_otica'][i] == 'nan' or df['competencia'][i] == 'nan' or df['saude_net_orig'][i] == 'nan':
                s = 'ROW REMOVED '
                linha = df.iloc[i]
                repasse_error = repasse_error.append(
                    linha, ignore_index=True, verify_integrity=False)
                s += ':'.join(df.iloc[i].values)
                logger.error(s)
                df.drop(df.index[i], inplace=True)
                n -= 1
                flag = True
            if not flag:
                i += 1
            else:
                flag = False
                df = df.reset_index(drop=True)

        generate_michael(repasse_error, 'repasse_error')

    return df


# function to extract xlsx files
def extract_xlsx(path, cols: any, log_id):
    logger = logging.getLogger(args.level + '.extracting')

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
        file = 'json/' + args.level + '/{}.json'.format(df_list[index])
        df = extract_xlsx(pathing[key], columns[key], log_id)
        df.to_json(file, orient='records',
                   date_format='iso', force_ascii=False, )
        index += 1

    return df


def generate_michael(df, file_name):
    file = 'json/' + args.level + '/{}.json'.format(file_name)

    df.to_json(file, orient='records',
               date_format='iso', force_ascii=False, )

    return df


def mongo_insert_many(list, user="", passw="", db=db, col=collection, data_frame=None):
    logger = logging.getLogger(args.level + '.inserting')

    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)

    log_everything(logger, data_frame)


def mongo_insert_log(list, user="", passw="", db=db, col=collection, data_frame=None):
    logger = logging.getLogger(args.level + '.inserting')

    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)


def mongo_insert_error(list, user="", passw="", db=db, col=collection):
    logger = logging.getLogger(args.level + '.inserting')

    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)


def run_loader(df):
    with open('json/' + args.level + '/header.json', encoding='utf-8') as file_header:
        header = json.load(file_header)

    with open('json/' + args.level + '/mensalidade.json', encoding='utf-8') as file_monthly:
        monthly_pay = json.load(file_monthly)

    with open('json/' + args.level + '/repasse.json', encoding='utf-8') as file_transfer:
        transfer = json.load(file_transfer)

    # insert data in MongoDB collections
    mongo_insert_many(list=header, db="cate", col="header_" +
                                                  (args.level), data_frame=df)
    mongo_insert_many(list=monthly_pay, db="cate", col="mensalidade_" +
                                                       (args.level), data_frame=df)
    mongo_insert_many(list=transfer, db="cate", col="repasse_" +
                                                    (args.level), data_frame=df)


def throw_away(df):
    if args.level != 'bronze':
        return
    else:
        with open('json/' + args.level + '/header_error.json', encoding='utf-8') as file_header:
            header_error = json.load(file_header)

        with open('json/' + args.level + '/mensalidade_error.json', encoding='utf-8') as file_monthly:
            mensalidade_error = json.load(file_monthly)

        with open('json/' + args.level + '/repasse_error.json', encoding='utf-8') as file_transfer:
            repasse_error = json.load(file_transfer)

        if header_error != []:
            mongo_insert_error(list=header_error, db="bronze_anomalies",
                               col="header_" + (args.level))
        if mensalidade_error != []:
            mongo_insert_error(list=mensalidade_error, db="bronze_anomalies",
                               col="mensalidade_" + (args.level))
        if repasse_error != []:
            mongo_insert_error(list=repasse_error, db="bronze_anomalies",
                               col="repasse_" + (args.level))
# END BRONZE



# BEG SILVER
def analysis():
    logger = logging.getLogger("silver.inserting")
    print("Conferindo base header...")
    bd = "cate"
    # Mostra as colunas e linhas da base header
    header = mongo_find_all(db="cate", col="header_bronze")
    df_header = pd.DataFrame(header)
    print(f"linhas: {len(df_header.index)}")
    df_header.head(5)

    # In[2]:

    # Mostra os tipos de dados de cada coluna
    df_header.dtypes

    # In[3]:

    # Transforma o numero_fatura e contrato em número
    df_header["numero_fatura"] = pd.to_numeric(df_header["numero_fatura"])
    df_header["contrato"] = pd.to_numeric(df_header["contrato"])
    df_header.dtypes

    # In[4]:

    # Verifica se tem algum valor nulo
    df_header.isnull().sum()

    # In[5]:

    print("Conferindo base mensalidade...")
    # Mostra as colunas e linhas da base mensalidade
    mensalidade = mongo_find_all(db="cate", col="mensalidade_bronze")
    df_mensalidade = pd.DataFrame(mensalidade)
    print(f"linhas: {len(df_mensalidade.index)}")
    df_mensalidade.head(5)

    # In[6]:

    df_mensalidade.dtypes

    # In[7]:

    # Transforma algumas colunas em número
    df_mensalidade["marca_otica"] = pd.to_numeric(df_mensalidade["marca_otica"])
    df_mensalidade["valor_orig"] = pd.to_numeric(df_mensalidade["valor_orig"])
    df_mensalidade.dtypes

    # In[8]:

    # Verifica se tem algum valor nulo
    df_mensalidade.isnull().sum()

    # In[9]:

    print("Após juntar header e mensalidade...")
    # Une as duas bases usando o _idFile
    df_header_mensalidade = pd.merge(df_header, df_mensalidade, on="_idFile")
    print(f"linhas: {len(df_header_mensalidade.index)}")
    df_header_mensalidade.isnull().sum()

    # In[10]:

    # Mostra repetições
    print(f"qtd de marca_otica contando repetições: {df_header_mensalidade['marca_otica'].value_counts().sum()}")
    df_header_mensalidade["marca_otica"].value_counts().head(15)

    # In[11]:

    repeticoes = df_header_mensalidade["marca_otica"].value_counts().loc[lambda x: x > 1]
    df = df_header_mensalidade["marca_otica"].isin(repeticoes.index)
    df_ = df_header_mensalidade[df]
    indexes = df_.index
    competencias = []
    marcas = []
    idx_r = []
    idx = []
    helper = -1
    for i in indexes:
        if df_.loc[i]["marca_otica"] not in marcas:
            marcas.append(df_.loc[i]["marca_otica"])
            competencias.append(df_.loc[i]["dt_competencia"])
            helper += 1
            idx.append(i)
        else:  # Dentre as repetições iniciais, procura pela mesma marca_otica mas com dt_competencia diferente
            if df_.loc[i]["dt_competencia"].strip() != competencias[marcas.index(df_.loc[i]["marca_otica"])].strip():
                df_.loc[i]["marca_otica"] = df_.loc[i]["marca_otica"] * 10000 + helper
                marcas.append(df_.loc[i]["marca_otica"])
                competencias.append(df_.loc[i]["dt_competencia"])
                helper += 1
                idx.append(i)
            else:
                idx_r.append(i)
    # Mantém somente as repetições e coloca no banco
    df_r = df_.drop(idx)
    df_r["resultado"] = "mensalidade_marca_otica_repetida"
    mongo_insert_many(df_r.to_dict("records"), db=bd, col="h_m_r_silver", data_frame=df_r)
    df_r

    # In[12]:

    # Transforma a marca_otica no index e remove suas repetições, deixando no mínimo 1 de cada
    df_header_mensalidade_umarca = df_header_mensalidade.groupby("marca_otica").nth[0]
    df_header_mensalidade_umarca.index.value_counts()

    # In[13]:

    # Tenta inserir as linhas extraidas no tratamento das repetições baseado na dt_competencia
    df_ = df_.drop(idx_r)
    df_u = df_.groupby("marca_otica").nth[0]
    df_u
    df_header_mensalidade_umarca.append(df_u)
    df_header_mensalidade_umarca

    # Mostra as colunas e qtd de linhas da base repasse
    repasse = mongo_find_all(db="cate", col="repasse_bronze")
    df_repasse = pd.DataFrame(repasse)
    print(f"linhas: {len(df_repasse.index)}")
    df_repasse.head(5)

    # In[14]:

    df_repasse.dtypes

    # In[15]:

    # Transforma algumas colunas em número
    # df_repasse["codigo_convenio"] = pd.to_numeric(df_repasse["codigo_convenio"])
    df_repasse["codigo_plano"] = pd.to_numeric(df_repasse["codigo_plano"])
    df_repasse["marca_otica"] = pd.to_numeric(df_repasse["marca_otica"])
    df_repasse["saude_net_orig"] = pd.to_numeric(df_repasse["saude_net_orig"])
    df_repasse.dtypes

    # In[16]:

    # Separa as linhas repetidas, mantendo pelo menos um registro das repetições
    mask = df_repasse.marca_otica.duplicated()
    print(f"Linhas repetidas: {len(df_repasse[mask])}")
    print(f"Linhas únicas: {len(df_repasse[~mask])}")
    df_repasse_u = df_repasse[~mask].copy()

    # In[17]:

    # Analisa a coluna de contrato
    df_repasse_u["cod_contrato"].value_counts()

    # In[18]:

    print("Removendo linhas sem contrato...")
    # Remove as linhas sem contrato ou com valor 0
    # dados_insuficientes = df_repasse_u[df_repasse_u.cod_contrato == "nan"]
    df = df_repasse_u.loc[df_repasse_u["cod_contrato"].isin(["nan", "0.0"])]
    df["resultado"] = "dados_insuficientes"
    df
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver", data_frame=df)
    df_repasse_u = df_repasse_u[df_repasse_u.cod_contrato != "nan"]
    df_repasse_u = df_repasse_u[df_repasse_u.cod_contrato != "0.0"]
    print(f"linhas: {len(df_repasse_u.index)}")
    df_repasse_u["cod_contrato"].value_counts()

    # In[19]:

    df_repasse_u["cod_contrato"] = pd.to_numeric(df_repasse_u["cod_contrato"])
    df_repasse_u.dtypes

    # In[20]:

    print("Verificando marca_otica de repasse com a união header x mensalidade...")
    # Verifica se a marca_otica do repasse está presente no cruzamento header_mensalidade
    counter = 0
    marcas_match = []
    for marca_repasse in df_repasse_u["marca_otica"]:
        if marca_repasse in df_header_mensalidade_umarca.index:  # marca_otica presente na header_mensalidade
            counter += 1
            marcas_match.append(marca_repasse)
    print(marcas_match)
    print(f"qtd de marca_otica que deu match: {counter}")

    # In[21]:

    # Deixa separado em um novo df apenas as linhas com marca_otica que deu match
    match = df_repasse_u["marca_otica"].isin(marcas_match)
    df = df_repasse_u.copy()[~match]
    df["resultado"] = "somente_repasse"
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver", data_frame=df)
    df_repasse_m = df_repasse_u.copy()[match]
    df_repasse_m["marca_otica"].value_counts()

    # In[22]:

    # print("Verificando os contratos...")
    # # Verifica os cod_contrato que estão presentes na base header_mensalidade
    # counter = 0
    # contrato_unmatch = []
    # contrato_match = []
    # for contrato_repasse in df_repasse_m["cod_contrato"]:
    #     search = df_header_mensalidade[df_header_mensalidade["contrato"] == contrato_repasse]
    #     if len(search.isnull().values) >= 1:
    #         counter += 1
    #         if contrato_repasse not in contrato_match:
    #             contrato_match.append(contrato_repasse)
    #     else:
    #         if contrato_repasse not in contrato_unmatch:
    #             contrato_unmatch.append(contrato_repasse)
    # print(contrato_unmatch)
    # print(f"qtd de match: {counter}")

    # In[24]:

    # # Separa em um df apenas os matches de contrato
    # match = df_repasse_m["cod_contrato"].isin(contrato_match)
    # df = df_repasse_m.copy()[~match]
    # df["resultado"] = "somente_repasse"
    # mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver")
    # df_repasse_f = df_repasse_m.copy()[match]
    # df_repasse_f["cod_contrato"]

    # In[24]:

    # Une as base header_mensalidade com a repasse
    df_repasse_umarca = df_repasse_m.groupby("marca_otica").nth(0)
    df_h_m_r = pd.merge(df_header_mensalidade_umarca, df_repasse_umarca, on="marca_otica")
    df_h_m_r

    print("Verficando a competencia...")
    # Verifica se a competencia é a mesma entre mensalidade e repasse
    for i, row in df_h_m_r.iterrows():
        if row["dt_competencia"].strip()[:7] == row["competencia"].strip()[:7]:
            df_h_m_r.loc[i, "resultado_competencia"] = "mes_conciliado"
        else:
            df_h_m_r.loc[i, "resultado_competencia"] = "mes_divergente"
    df_h_m_r

    print("Verifica dados do header x mensalidades que não estão no repasse...")
    # Verifica as linhas que estavam no header_mensalidade mas não estavam no repasse
    counter = 0
    marcas_lost = []
    df = df_header_mensalidade_umarca.reset_index(level=0)
    for i in df_header_mensalidade_umarca.index:
        if i not in df_h_m_r.index:
            marcas_lost.append(i)
        counter += 1
    print(marcas_lost)
    print(f"qtd de linhas perdidas: {len(marcas_lost)}")

    # In[26]:

    # Insere no mongo
    match = df["marca_otica"].isin(marcas_lost)
    df = df[match]
    df["resultado"] = "somente_mensalidade"
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver", data_frame=df)
    df

    # In[27]:

    # print("Com as 3 bases.. confere se o contrato é o mesmo...")
    # # Analisa se o contrato da match, afinal eles são os campos chave entre header e repasse
    # # Salva num dict alguns dados relevantes sobre essa anomalia
    # contrato_unmatch = {"header_mensalidade": [], "repasse": [], "tp_beneficiario": [], "marca_otica": [], "rubrica": []}
    # for i in df_h_m_r.index:    # contrato vem do header_mensalidade e cod_contrato do repasse
    #     if df_h_m_r["contrato"].at[i] != df_h_m_r["cod_contrato"].at[i]:
    #         contrato_unmatch["header_mensalidade"].append(df_h_m_r["contrato"].at[i])
    #         contrato_unmatch["repasse"].append(df_h_m_r["cod_contrato"].at[i])
    #         contrato_unmatch["tp_beneficiario"].append(df_h_m_r["tp_beneficiario"].at[i])
    #         contrato_unmatch["marca_otica"].append(i)
    #         contrato_unmatch["rubrica"].append(df_h_m_r["rubrica"].at[i])
    #         df_h_m_r.loc[i]["resultado"] = "dados inconsistentes"
    #         mongo_insert_one(df_h_m_r.loc[i].to_dict(), db=bd, col="h_m_r_silver")
    #         df_h_m_r.drop(i, inplace=True)
    # contrato_unmatch

    # In[28]:

    df_h_m_r

    # In[29]:

    # Analisa quem é dependente
    dependentes = []
    for i in df_h_m_r.index:
        if df_h_m_r["tp_beneficiario"].at[i] == "D":
            dependentes.append(i)
    print(f"qtd: {len(dependentes)}")
    dependentes

    # In[30]:

    print("Amostra a seguir...")
    # Apenas uma amostra
    counter = 0
    divergencias = {"valor_orig", "val"}
    for i in df_h_m_r.index:
        if int(df_h_m_r["valor_orig"].at[i]) != int(df_h_m_r["saude_net_orig"].at[i]):
            # if "Capita" in df_h_m_r["rubrica"].at[i]:
            if df_h_m_r["tp_beneficiario"].at[i] == "D":
                print("$" * 30)
            counter += 1
            # print(df_h_m_r.loc[i])
            print(f"marca otica          - {i}")
            print(f"rubrica              - {df_h_m_r['rubrica'].at[i]}")
            print(f"tipo de beneficiario - {df_h_m_r['tp_beneficiario'].at[i]}")
            print(f"contrato             - {df_h_m_r['contrato'].at[i]}")
            # print(f"outros              - {df_h_m_r['outros'].at[i]}")
            print(f"valor orig           - {int(df_h_m_r['valor_orig'].at[i])}")
            print(f"saude net orig       - {int(df_h_m_r['saude_net_orig'].at[i])}")
            print(f"situacao             - {df_h_m_r['situacao'].at[i]}")
            print(f"cancelamento         - {df_h_m_r['dt_cancelamento'].at[i]}")
            print(f"competencia          - {df_h_m_r['competencia'].at[i]}")
            print("-" * 30)
    print(counter)

    # In[31]:

    print("Conferindo se os valores são os mesmos...")
    # Identifica todos os casos conciliados e conciliados_com_div
    df_h_m_r = df_h_m_r.reset_index()

    bd = "cate"
    matches = []
    results = []
    for i in df_h_m_r.index:
        if int(df_h_m_r["valor_orig"].at[i]) == int(df_h_m_r["saude_net_orig"].at[i]):
            # df_h_m_r.drop(i, inplace=True)
            matches.append(i)
            results.append("conciliado")
        elif df_h_m_r["valor_orig"].at[i] < 0 and "Retroativa" in df_h_m_r["rubrica"].at[i]:
            print(i)
            matches.append(i)
            results.append("conciliado")
        else:
            results.append("conciliado_com_div")
    df_h_m_r["resultado"] = results
    mongo_insert_many(df_h_m_r.to_dict("records"), db=bd, col="h_m_r_silver", data_frame=df_h_m_r)
    print(matches)
    print(f"qtd: {len(matches)}")

    return "OK"
# END SILVER




# BEG GOLD
def to_date(x):
    if x == 'NaT' or x is None or x == 'nan':
        return None

    x = x[:10]
    return datetime.strptime(x, '%Y-%m-%d')


def get_id():
    return cursor.lastrowid


def to_int(x):
    if x is None or x == 'nan':
        return None
    return int(x)


def to_float(x):
    if x is None or x == 'nan':
        return None
    return float(x)


def build_fatura(line):
    fatura = {}
    colunas = ['dt_competencia', 'numero_fatura', 'rubrica', 'parcela_1', 'dt_geracao']
    for coluna in colunas:
        if coluna not in line.keys():
            line[coluna] = None

    fatura['competencia'] = to_date(line['dt_competencia'])
    fatura['numero_fatura'] = to_int(line['numero_fatura'])
    fatura['rubrica'] = line['rubrica']
    fatura['parcela'] = to_float(line['parcela_1'])
    fatura['dt_geracao'] = to_date(line['dt_geracao'])

    return fatura


def save_faturas(fatura):
    sql = "insert into dw.fatura(competencia, numero_fatura, rubrica , parcela, dt_geracao ) " \
          "values(%s,%s,%s,%s,%s) "

    cursor.execute(sql,
                   [fatura['competencia'],
                    fatura['numero_fatura'],
                    fatura['rubrica'],
                    fatura['parcela'],
                    fatura['dt_geracao']])

    api_db.commit()
    return get_id()


def build_convenio(line):
    convenio = {}
    colunas = ['codigo_convenio', 'convenio', 'operadora']
    for coluna in colunas:
        if coluna not in line.keys():
            line[coluna] = None

    convenio['codigo_convenio'] = to_int(line['codigo_convenio'])
    convenio['convenio'] = line['convenio']
    convenio['operadora'] = line['operadora']

    return convenio


def save_convenio(convenio):
    sql = "insert into dw.convenio(codigo_convenio, convenio, operadora) " \
          "values(%s,%s,%s) "

    cursor.execute(sql,
                   [convenio['codigo_convenio'],
                    convenio['convenio'],
                    convenio['operadora']])

    api_db.commit()
    return get_id()


def build_beneficiario(line):
    beneficiario = {}
    colunas = ['tp_beneficiario', 'nXmX_bXnXfiWiXriX', 'marca_otica', 'dt_nascimento']
    for coluna in colunas:
        if coluna not in line.keys():
            line[coluna] = None

    beneficiario['tipo'] = line['tp_beneficiario']
    beneficiario['nome'] = line['nXmX_bXnXfiWiXriX']
    beneficiario['marca_otica'] = to_int(line['marca_otica'])
    beneficiario['dt_nascimento'] = to_date(line['dt_nascimento'])

    return beneficiario


def save_beneficiario(beneficiario):
    sql = "insert into dw.benificiario(tipo, nome, marca_otica, dt_nascimento) " \
          "values(%s,%s,%s,%s) "

    cursor.execute(sql,
                   [beneficiario['tipo'],
                    beneficiario['nome'],
                    beneficiario['marca_otica'],
                    beneficiario['dt_nascimento']])

    api_db.commit()
    return get_id()


def build_data():
    data = {}

    data['data'] = datetime.now()

    return data


def save_data(data):
    sql = "insert into dw.tempo(data) " \
          "values(%s) "

    cursor.execute(sql,
                   [data['data']])

    api_db.commit()
    return get_id()


def build_contrato(line):
    contrato = {}
    colunas = ['plano_x', 'contrato', 'situacao', 'dependente', 'dt_cancelamento', 'dt_situacao', 'inicio_vigencia',
               'dt_suspensao']
    for coluna in colunas:
        if coluna not in line.keys():
            if coluna == 'dependente':
                line[coluna] = 0
            else:
                line[coluna] = None

    contrato['plano'] = line['plano_x']
    contrato['num_contrato'] = to_int(line['contrato'])
    contrato['situacao'] = line['situacao']
    contrato['dependente'] = to_int(line['dependente'])
    contrato['dt_cancelamento'] = to_date(line['dt_cancelamento'])
    contrato['dt_situacao'] = to_date(line['dt_situacao'])
    contrato['inicio_vigencia'] = to_date(line['inicio_vigencia'])
    contrato['dt_suspensao'] = to_date(line['dt_suspensao'])

    return contrato


def save_contrato(contrato):
    sql = "insert into dw.contrato(plano, num_contrato, situacao, " \
          "dependente, dt_cancelamento, dt_situacao, inicio_vigencia, dt_suspensao ) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s) "

    cursor.execute(sql,
                   [contrato['plano'],
                    contrato['num_contrato'],
                    contrato['situacao'],
                    contrato['dependente'],
                    contrato['dt_cancelamento'],
                    contrato['dt_situacao'],
                    contrato['inicio_vigencia'],
                    contrato['dt_suspensao']])

    api_db.commit()
    return get_id()


def build_fato(line, ids):
    fato = {}
    colunas = ['valor_orig', 'saude_net_orig', 'resultado']
    for coluna in colunas:
        if coluna not in line.keys():
            if coluna == 'resultado':
                line[coluna] = None
            else:
                line[coluna] = 0

    fato['id_cont'] = ids['id_cont']
    fato['id_fat'] = ids['id_fat']
    fato['id_seg'] = ids['id_seg']
    fato['id_dat'] = ids['id_dat']
    fato['id_conv'] = ids['id_conv']
    fato['mensalidade'] = line['valor_orig']
    fato['repasse'] = line['saude_net_orig']
    fato['caso'] = line['resultado']

    return fato


def save_fato(fato):
    sql = "insert into dw.fato(id_cont, id_fat, id_seg , " \
          "id_dat, id_conv, mensalidade, repasse, caso ) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.execute(sql,
                   [fato['id_cont'],
                    fato['id_fat'],
                    fato['id_seg'],
                    fato['id_dat'],
                    fato['id_conv'],
                    fato['mensalidade'],
                    fato['repasse'],
                    fato['caso']]
                   )

    api_db.commit()


def execute_etl(lines):
    ids = {}

    for line in lines:
        fatura = build_fatura(line)
        ids['id_fat'] = save_faturas(fatura)

        contrato = build_contrato(line)
        ids['id_cont'] = save_contrato(contrato)

        convenio = build_convenio(line)
        ids['id_conv'] = save_convenio(convenio)

        beneficiario = build_beneficiario(line)
        ids['id_seg'] = save_beneficiario(beneficiario)

        data = build_data()
        ids['id_dat'] = save_data(data)

        fato = build_fato(line, ids)
        save_fato(fato)


def execute_gold():
    lines = mongo_find_all(db="cate", col="h_m_r_silver")

    try:
        execute_etl(lines)
    except Exception as e:
        print(e.with_traceback())
    finally:
        cursor.close()
        api_db.close()

# END GOLD




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
    start_time = datetime.now()
    start_time_file = str(start_time).replace(':', '-')
    start_time_file = str(start_time_file).replace(' ', '_')
    log_id = uuid.uuid1()
    log_id = str(start_time_file) + '_' + str(log_id)

    def execute_bronze():
        sys.stdout.write('Starting Bronze procedure...\n')
        df = ''
        setup_logger(log_id)
        sys.stdout.write('Executing Bronze procedure...\n')
        sys.stdout.write('Generating json\n')
        df = generate_json(df, log_id)

        sys.stdout.write('Inserting into mongo...\n')

        throw_away(df)
        run_loader(df)
        save_logging(log_id)

    def execute_silver():
        sys.stdout.write('Starting Silver procedure...\n')
        setup_logger(log_id)
        sys.stdout.write('Executing Silver procedure...\n')
        analysis()
        sys.stdout.write('Inserting into mongodb...\n')

    if args.level == 'bronze':
        execute_bronze()

    elif args.level == "silver":
        execute_silver()

    elif args.level == 'gold':
        sys.stdout.write('Starting Gold procedure...\n')
        sys.stdout.write('Executing Gold procedure...\n')
        execute_gold()
        sys.stdout.write('Data inserted into Data Warehouse!\n')

    elif args.level == 'all':
        args.level = 'bronze'
        execute_bronze()

        args.level = 'silver'
        execute_silver()

        args.level = 'gold'
        execute_gold()

    sys.stdout.write('Inserting log into mongodb...\n')
    save_logging(log_id)

    end_time = datetime.now()

    sys.stdout.write('--- duration: {} ---\n'.format(end_time - start_time))


if __name__ == '__main__':
    try:
        main()
    except:
        traceback.print_exc()
    finally:
        # delete_log.delete_log()
        cursor.close()
        api_db.close()
