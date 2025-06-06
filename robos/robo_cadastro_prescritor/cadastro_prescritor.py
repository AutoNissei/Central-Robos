import requests
import re
import os
from dotenv import load_dotenv
from . import fun_cadastro_prescritor as fun


def run(ativo):
    log = fun.get_logger("cadastro_de_prescritor")
    load_dotenv()

    # === 1. Autenticação Desk.ms ===
    try:
        url_auth_desk = "https://api.desk.ms/Login/autenticar"
        headers_desk = {
            "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
            "JsonPath": "true"
        }
        payload_desk = {
            "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
        }

        response_desk = requests.post(url_auth_desk, json=payload_desk, headers=headers_desk)
        response_desk.raise_for_status()

        token_desk = response_desk.json().get("access_token")
        if not token_desk:
            log("❌ Token da API Desk.ms não foi retornado.")
            return

        log("✅ Autenticação Desk.ms realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro na autenticação com Desk.ms: {e}")
        return

    # === 2. Autenticação DataHub ===
    try:
        url_auth_datahub = "https://datahub-api.nisseilabs.com.br/auth/token"
        payload_datahub = {
            "grant_type": "password",
            "username": "joao.novitzki",
            "password": "f7KMy5Lj4yxAW8"
        }
        headers_datahub = {"Content-Type": "application/x-www-form-urlencoded"}

        response_datahub = requests.post(url_auth_datahub, data=payload_datahub, headers=headers_datahub)
        response_datahub.raise_for_status()

        token_datahub = response_datahub.json().get("access_token")
        if not token_datahub:
            log("❌ Token da API DataHub não foi retornado.")
            return

        log("✅ Autenticação DataHub realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro na autenticação com DataHub: {e}")
        return

    # === 3. Listagem de chamados ===
    try:
        url_chamados = "https://api.desk.ms/ChamadosSuporte/lista"
        headers_chamados = {"Authorization": f"{token_desk}"}
        payload_chamados = {
            "Pesquisa": "CSN - CADASTRO DE PRESCRITOR",
            "Tatual": "",
            "Ativo": ativo,
            "StatusSLA": "",
            "Colunas": {
                "Chave": "on",
                "CodChamado": "on",
                "NomeUsuario": "on",
                "Descricao": "on",
                "_126143": "on",  # CRM
                "_126157": "on",  # UF
                "_126152": "on"   # Tipo de prescritor
            },
            "Ordem": [
                {
                    "Coluna": "Chave",
                    "Direcao": "true"
                }
            ]
        }

        response_chamados = requests.post(url_chamados, json=payload_chamados, headers=headers_chamados)
        response_chamados.raise_for_status()

        chamados = response_chamados.json().get("root", [])
        if not chamados:
            log("ℹ️ Nenhum chamado encontrado.")
            return

        # === Processamento dos chamados ===
        for chamado in chamados:
            cod_chamado = chamado["CodChamado"]
            descricao = chamado.get("Descricao", "")
            cod_cr = chamado.get("_126143", "").strip()
            uf_prescritor = chamado.get("_126157", "")
            tipo_cr = chamado.get("_126152", "")
            nome_usuario = chamado.get("NomeUsuario", "")

            # === UF ===
            match_uf = re.search(r'\b[A-Z]{2}\b', uf_prescritor)
            if not match_uf:
                log(f"⚠️ UF não identificada no chamado {cod_chamado}. Pulando...")
                continue
            uf = match_uf.group()

            # === Filial ===
            match_filial = re.search(r"\d+", nome_usuario)
            if not match_filial:
                log(f"⚠️ Número da filial não identificado no chamado {cod_chamado}. Pulando...")
                continue
            num_filial = int(match_filial.group())

            log(f"Chamado: {cod_chamado} | CR: {cod_cr} | UF: {uf} | Tipo: {tipo_cr} | Filial: {num_filial}")

            try:
                # === Cadastro do prescritor ===
                mensagem, tipo_cr_result = fun.cadastrar_prescritor(uf, cod_cr, tipo_cr, token_datahub)

                # === Interagir no chamado ===
                fun.interagir_chamado(cod_chamado, token_desk, mensagem, tipo_cr_result)

            except Exception as e:
                log(f"❌ Erro ao processar o chamado {cod_chamado}: {e}")

    except Exception as e:
        log(f"❌ Erro ao listar/processar chamados: {e}")
