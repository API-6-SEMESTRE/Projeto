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
warnings.filterwarnings("ignore", category=FutureWarning)
from silver_analysis import analysis


header = mongo_find_all(db="cate", col="header_bronze")


def save_faturas(lines):
    faturas = {}
    for i in lines:
        print(i)
        faturas['competencia'] = i['competencia']
        faturas['numero_fatura'] = i['numero_fatura']
        faturas['rubrica'] = i['rubrica']
        faturas['parcela'] = i['parcela_1']
        faturas['dt_geracao'] = i['dt_geracao']
        print(faturas)
        break


def save_convenio(lines):
    convenios = {}
    for i in lines:
        convenios['codigo_convenio'] = i['codigo_convenio']
        convenios['convenio'] = i['convenio']
        convenios['operadora'] = i['operadora']


def save_beneficiario(lines):
    beneficiarios = {}
    for i in lines:
        beneficiarios['tipo'] = i['tp_beneficiario']
        beneficiarios['nome'] = i['nXmX_bXnXfiWiXriX']
        beneficiarios['marca_otica'] = i['marca_otica']
        beneficiarios['dt_nascimento'] = i['dt_nascimento']


def save_data(lines):
    data = {}
    data['data'] = datetime.now()


def save_contrato(lines):
    contratos={}
    for i in lines:
        contratos['plano'] = i['plano_x']
        contratos['num_contrato'] = i['contrato']
        contratos['situacao'] = i['situacao']
        contratos['dependente'] = i['dependente']
        contratos['dt_cancelamento']=i['dt_cancelamento']
        contratos['dt_situacao']=i['dt_situacao']
        contratos['inicio_vigencia']=i['inicio_vigencia']
        contratos['dt_suspensao']=i['dt_suspensao']


def execute_gold():
    lines = mongo_find_all(db="cate", col="h_m_r_silver")
    save_faturas(lines)


execute_gold()
