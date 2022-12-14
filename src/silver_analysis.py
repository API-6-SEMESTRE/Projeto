#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from mongo_portal import mongo_find_all, mongo_insert_one, mongo_insert_many


def analysis():
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
    df_["resultado"] = "mensalidade_marca_otica_repetida"
    mongo_insert_many(df_.to_dict("records"), db=bd, col="h_m_r_silver")
    df_

    # In[12]:

    # Transforma a marca_otica no index e remove suas repetições, deixando no mínimo 1 de cada
    df_header_mensalidade_umarca = df_header_mensalidade.groupby("marca_otica").nth[0]
    df_header_mensalidade_umarca.index.value_counts()

    # In[13]:

    # Mostra as colunas e qtd de linhas da base repasse
    repasse = mongo_find_all(db="cate", col="repasse_bronze")
    df_repasse = pd.DataFrame(repasse)
    print(f"linhas: {len(df_repasse.index)}")
    df_repasse.head(5)

    # In[14]:

    df_repasse.dtypes

    # In[15]:

    # Transforma algumas colunas em número
    df_repasse["codigo_convenio"] = pd.to_numeric(df_repasse["codigo_convenio"])
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
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver")
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
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver")
    df_repasse_m = df_repasse_u.copy()[match]
    df_repasse_m["marca_otica"].value_counts()

    # In[22]:

    print("Verifica os cod_contrato...")
    # Verifica os cod_contrato que estão presentes na base header_mensalidade
    counter = 0
    contrato_unmatch = []
    contrato_match = []
    for contrato_repasse in df_repasse_m["cod_contrato"]:
        search = df_header_mensalidade[df_header_mensalidade["contrato"] == contrato_repasse]
        if len(search.isnull().values) >= 1:
            counter += 1
            if contrato_repasse not in contrato_match:
                contrato_match.append(contrato_repasse)
        else:
            if contrato_repasse not in contrato_unmatch:
                contrato_unmatch.append(contrato_repasse)
    print(contrato_unmatch)
    print(f"qtd de match: {counter}")

    # In[23]:

    # Separa em um df apenas os matches de contrato
    match = df_repasse_m["cod_contrato"].isin(contrato_match)
    df = df_repasse_m.copy()[~match]
    df["resultado"] = "somente_repasse"
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver")
    df_repasse_f = df_repasse_m.copy()[match]
    df_repasse_f["cod_contrato"]

    # In[24]:

    # Une as base header_mensalidade com a repasse
    df_repasse_umarca = df_repasse_f.groupby("marca_otica").nth(0)
    df_h_m_r = pd.merge(df_header_mensalidade_umarca, df_repasse_umarca, on="marca_otica")
    df_h_m_r

    # In[25]:

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
    mongo_insert_many(df.to_dict("records"), db=bd, col="h_m_r_silver")
    df

    # In[27]:

    print("Com as 3 bases.. confere se o contrato é o mesmo...")
    # Analisa se o contrato da match, afinal eles são os campos chave entre header e repasse
    # Salva num dict alguns dados relevantes sobre essa anomalia
    contrato_unmatch = {"header_mensalidade": [], "repasse": [], "tp_beneficiario": [], "marca_otica": [],
                        "rubrica": []}
    for i in df_h_m_r.index:  # contrato vem do header_mensalidade e cod_contrato do repasse
        if df_h_m_r["contrato"].at[i] != df_h_m_r["cod_contrato"].at[i]:
            contrato_unmatch["header_mensalidade"].append(df_h_m_r["contrato"].at[i])
            contrato_unmatch["repasse"].append(df_h_m_r["cod_contrato"].at[i])
            contrato_unmatch["tp_beneficiario"].append(df_h_m_r["tp_beneficiario"].at[i])
            contrato_unmatch["marca_otica"].append(i)
            contrato_unmatch["rubrica"].append(df_h_m_r["rubrica"].at[i])
            mongo_insert_one(df_h_m_r.loc[i].to_dict(), db=bd, col="dados_inconsistentes")
            df_h_m_r.drop(i, inplace=True)
    contrato_unmatch

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
            results.append("conciliados")
        else:
            results.append("conciliados_com_div")
    df_h_m_r["resultado"] = results
    mongo_insert_many(df_h_m_r.to_dict("records"), db=bd, col="h_m_r_silver")
    print(matches)
    print(f"qtd: {len(matches)}")

    return "OK"
