import sys
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from mongo_portal import mongo_find_all
import argparse
from datetime import datetime
import uuid
import json
import logging
import time
import warnings
import oracledb

warnings.filterwarnings("ignore", category=FutureWarning)
from silver_analysis import analysis

p_username = "ADMIN"
p_password = "rnVusRjcU2naBa"
p_walletpass = "rnVusRjcU2naBa"

con = oracledb.connect(user=p_username,
                       password=p_password,
                       dsn="apicatedw_high",
                       config_dir="./key",
                       wallet_location="./key",
                       wallet_password=p_walletpass)
header = mongo_find_all(db="cate", col="header_bronze")
cursor = con.cursor()


def to_date(x):
    if x == 'NaT':
        return None
    return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')


def to_id(x):
    return int(x.getvalue()[0])

def build_fatura(line):
    fatura = {}

    fatura['competencia'] = to_date(line['competencia'])
    fatura['numero_fatura'] = int(line['numero_fatura'])
    fatura['rubrica'] = line['rubrica']
    fatura['parcela'] = float(line['parcela_1'])
    fatura['dt_geracao'] = to_date(line['dt_geracao'])

    return fatura


def save_faturas(fatura):
    id_fat = cursor.var(oracledb.NUMBER)

    sql = "insert into fatura(competencia, numero_fatura, rubrica , parcela, dt_geracao ) " \
          "values(:competencia,:numero_fatura,:rubrica,:parcela,:dt_geracao) " \
          "returning id_fat into :id_fat"

    cursor.execute(sql,
                   [fatura['competencia'],
                    fatura['numero_fatura'],
                    fatura['rubrica'],
                    fatura['parcela'],
                    fatura['dt_geracao'],
                    id_fat])

    print(id_fat.getvalue())
    con.commit()
    return to_id(id_fat)


def build_convenio(line):
    convenio = {}

    convenio['codigo_convenio'] = int(line['codigo_convenio'])
    convenio['convenio'] = line['convenio']
    convenio['operadora'] = line['operadora']

    return convenio


def save_convenio(convenio):
    id_conv = cursor.var(oracledb.NUMBER)

    sql = "insert into convenio(codigo_convenio, convenio, operadora) " \
          "values(:codigo_convenio,:convenio,:operadora) " \
          "returning id_conv into :id_conv"

    cursor.execute(sql,
                   [convenio['codigo_convenio'],
                    convenio['convenio'],
                    convenio['operadora'],
                    id_conv])

    print(id_conv.getvalue())
    con.commit()
    return to_id(id_conv)


def build_beneficiario(line):
    beneficiario = {}

    beneficiario['tipo'] = line['tp_beneficiario']
    beneficiario['nome'] = line['nXmX_bXnXfiWiXriX']
    beneficiario['marca_otica'] = int(line['marca_otica'])
    beneficiario['dt_nascimento'] = to_date(line['dt_nascimento'])

    return beneficiario


def save_beneficiario(beneficiario):
    id_seg = cursor.var(oracledb.NUMBER)

    print(beneficiario)
    sql = "insert into beneficiario(tipo, nome, marca_otica, dt_nascimento) " \
          "values(:tipo,:nome,:marca_otica,:dt_nascimento) " \
          "returning id_seg into :id_seg"

    cursor.execute(sql,
                   [beneficiario['tipo'],
                    beneficiario['nome'],
                    beneficiario['marca_otica'],
                    beneficiario['dt_nascimento'],
                    id_seg])

    print(id_seg.getvalue())
    con.commit()
    return to_id(id_seg)


def build_data():
    data = {}

    data['data'] = datetime.now()

    return data


def save_data(data):
    id_dat = cursor.var(oracledb.NUMBER)

    sql = "insert into tempo(data) " \
          "values(:data) " \
          "returning id_dat into :id_dat"

    cursor.execute(sql,
                   [data['data'],
                    id_dat])

    print(id_dat.getvalue())
    con.commit()
    return to_id(id_dat)


def build_contrato(line):
    contrato = {}

    contrato['plano'] = line['plano_x']
    contrato['num_contrato'] = int(line['contrato'])
    contrato['situacao'] = line['situacao']
    contrato['dependente'] = int(line['dependente'])
    contrato['dt_cancelamento'] = to_date(line['dt_cancelamento'])
    contrato['dt_situacao'] = to_date(line['dt_situacao'])
    contrato['inicio_vigencia'] = to_date(line['inicio_vigencia'])
    contrato['dt_suspensao'] = to_date(line['dt_suspensao'])

    return contrato


def save_contrato(contrato):
    id_cont = cursor.var(oracledb.NUMBER)

    sql = "insert into contrato(plano, num_contrato, situacao, " \
          "dependente, dt_cancelamento, dt_situacao, inicio_vigencia, dt_suspensao ) " \
          "values(:plano,:num_contrato,:situacao," \
          ":dependente,:dt_cancelamento,:dt_situacao,:inicio_vigencia,:dt_suspensao) " \
          "returning id_cont into :id_cont"

    cursor.execute(sql,
                   [contrato['plano'],
                    contrato['num_contrato'],
                    contrato['situacao'],
                    contrato['dependente'],
                    contrato['dt_cancelamento'],
                    contrato['dt_situacao'],
                    contrato['inicio_vigencia'],
                    contrato['dt_suspensao'],
                    id_cont])

    print(id_cont.getvalue())
    con.commit()
    return to_id(id_cont)


def build_fato(line, ids):
    fato = {}

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
    sql = "insert into fato(id_cont, id_fat, id_seg , " \
          "id_dat, id_conv, mensalidade, repasse, caso ) " \
          "values(:id_cont,:id_fat,:id_seg," \
          ":id_dat,:id_conv,:mensalidade,:repasse,:caso)"

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

    con.commit()


def execute_etl(lines):
    ids = {}

    for line in lines:
        #print(line)
        #sys.exit()
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
    #print(len(lines))
    #sys.exit()
    try:
        execute_etl(lines)
    except Exception as e:
        print(e.with_traceback())
    finally:
        cursor.close()
        con.close()


execute_gold()
