import sys
from mongo_portal import mongo_find_all
from datetime import datetime
import warnings
import mysql.connector

warnings.filterwarnings("ignore", category=FutureWarning)

api_db = mysql.connector.connect(
    host=<seuhost>,
    user=<seuuser>,
    password=<seupassword>,
    database='dw'
)

header = mongo_find_all(db="cate", col="header_bronze")
cursor = api_db.cursor()


def to_date(x):
    if x == 'NaT' or x is None or x == 'nan':
        return None
    return datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z')


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

    print(get_id())
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

    print(get_id())
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

    print(get_id())
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

    print(get_id())
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

    print(get_id())
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


execute_gold()
