import pyodbc
import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import logging

# === CONFIGURAÇÃO DO LOG ===
def get_logger(nome_robo: str):
    log_dir = os.path.join("logs", f"logs_{nome_robo}")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(f"logger_{nome_robo}")  # Usa nome único
    logger.setLevel(logging.INFO)

    # Remove handlers antigos se existirem
    if logger.hasHandlers():
        logger.handlers.clear()

    # Cria novo FileHandler e StreamHandler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger.info



log = get_logger("exclusao_protocolo")

load_dotenv()

central_db_config = {
    "server": os.getenv("CENTRAL_DB_SERVER"),
    "database": os.getenv("CENTRAL_DB_DATABASE"),
    "username": os.getenv("CENTRAL_DB_USER"),
    "password": os.getenv("CENTRAL_DB_PASS")
}

def obter_ip_filial(filial):
    if 1 <= filial <= 200 or filial == 241:
        ip = f"10.16.{filial}.24"
    elif 201 <= filial <= 299:
        ip = f"10.17.{filial % 100}.24"
    elif 300 <= filial <= 399:
        ip = f"10.17.1{filial % 100}.24"
    elif 400 <= filial <= 499:
        ip = f"10.18.{filial % 100}.24"
    elif filial == 247:
        ip = f"192.168.201.1"
    else:
        raise ValueError("Número de filial inválido.")

    filial_db_config = {
        "server": ip,
        "database": os.getenv("FILIAL_DB_DATABASE"),
        "username": os.getenv("FILIAL_DB_USER"),
        "password": os.getenv("FILIAL_DB_PASS")
    }

    return filial_db_config

def conectar_filial(num_filial):
    config_bd_filial = obter_ip_filial(num_filial)
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={config_bd_filial['server']};"
            f"DATABASE={config_bd_filial['database']};"
            f"UID={config_bd_filial['username']};"
            f"PWD={config_bd_filial['password']}"
        )
        return conn
    except Exception as e:
        log(f"Erro ao conectar ao banco da filial: {e}")
        return None

def conectar_central():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={central_db_config['server']};"
            f"DATABASE={central_db_config['database']};"
            f"UID={central_db_config['username']};"
            f"PWD={central_db_config['password']}"
        )
        return conn
    except Exception as e:
        log(f"Erro ao conectar ao banco central: {e}")
        return None

def ler_arquivo(nome_arquivo):
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, "r") as arquivo:
            return [linha.strip() for linha in arquivo.readlines() if linha.strip()]
    return []

def excluir_protocolo_central(chaves):
    protocolos_nao_encontrados_central = []
    try:
        conn = conectar_central()
        if conn is None:
            return []

        cursor = conn.cursor()

        for chave in chaves:
            cursor.execute("""
                SELECT PROTOCOLO_NAO_RECEBIMENTO_NF 
                FROM PROTOCOLOS_NAO_RECEBIMENTOS_NF 
                WHERE CHAVE_NFE = ?
            """, chave)
            resultado = cursor.fetchone()

            if resultado:
                try:
                    cursor.execute("""
                        DELETE FROM PROTOCOLOS_NAO_RECEBIMENTOS_NF 
                        WHERE CHAVE_NFE = ?
                    """, chave)
                    conn.commit()
                except Exception as e:
                    log(f"Erro ao excluir o protocolo da central para a chave {chave}: {e}")
            else:
                protocolos_nao_encontrados_central.append(chave)

        return protocolos_nao_encontrados_central

    except Exception as e:
        log(f"Erro ao realizar o processo de exclusão da central: {e}")
        return []

    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass


def excluir_protocolo_filial(chaves, num_filial, protocolos_nao_encontrados_central):
    protocolos_nao_encontrados = []

    try:
        conn_filial = conectar_filial(num_filial)
        if conn_filial is None:
            return []

        cursor = conn_filial.cursor()

        for chave in chaves:
            cursor.execute("""
                SELECT PROTOCOLO_NAO_RECEBIMENTO_NF 
                FROM PROTOCOLOS_NAO_RECEBIMENTOS_NF 
                WHERE CHAVE_NFE = ?
            """, chave)
            resultado = cursor.fetchone()

            if resultado:
                try:
                    cursor.execute("""
                        DELETE FROM PROTOCOLOS_NAO_RECEBIMENTOS_NF 
                        WHERE CHAVE_NFE = ?
                    """, chave)
                    conn_filial.commit()
                except Exception as e:
                    log(f"Erro ao excluir o protocolo da filial para a chave {chave}: {e}")
            else:
                if chave in protocolos_nao_encontrados_central:
                    protocolos_nao_encontrados.append(chave)

        return protocolos_nao_encontrados

    except Exception as e:
        log(f"Erro ao consultar notas na filial: {e}")
        return []

    finally:
        try:
            if cursor:
                cursor.close()
            if conn_filial:
                conn_filial.close()
        except:
            pass





def interagir_chamado(cod_chamado, token, protocolos_nao_encontrados):
    descricao = "Resumo da exclusão dos protocolos de não recebimento de notas fiscais\n\n"

    if protocolos_nao_encontrados:
        descricao += "*Foram excluídos os protocolos de não recebimento com excessão dos protocolos das seguintes notas que não foram localizados ou já foram excluídos: *\n" + "\n".join(protocolos_nao_encontrados) + "\n\n"
    else:
        descricao += "Todos os protocolos de não recebimento foram excluídos da central e da loja, segue para recebimento da nota."

    cod_status = "0000002"

    data_interacao = datetime.now().strftime("%d-%m-%Y")
    url = "https://api.desk.ms/ChamadosSuporte/interagir"

    payload = {
        "Chave": cod_chamado,
        "TChamado": {
            "CodFormaAtendimento": "1",
            "CodStatus": cod_status,
            "CodAprovador": [""],
            "TransferirOperador": "",
            "TransferirGrupo": "",
            "CodTerceiros": "",
            "Protocolo": "",
            "Descricao": descricao,
            "CodAgendamento": "",
            "DataAgendamento": "",
            "HoraAgendamento": "",
            "CodCausa": "000467",
            "CodOperador": "249",
            "CodGrupo": "",
            "EnviarEmail": "S",
            "EnvBase": "N",
            "CodFPMsg": "",
            "DataInteracao": data_interacao,
            "HoraInicial": "",
            "HoraFinal": "",
            "SMS": "",
            "ObservacaoInterna": "",
            "PrimeiroAtendimento": "S",
            "SegundoAtendimento": "N"
        },
        "TIc": {
            "Chave": {
                "278": "on",
                "280": "on"
            }
        }
    }

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    try:
        response = requests.put(url, json=payload, headers=headers)

        if response.status_code == 200:
                log(f"Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"Erro ao interagir no chamado. Código: {response.status_code}")
            log("Resposta da API:")
            log(response.text)
            try:
                erro_json = response.json()
                log(f"Detalhes do erro: {erro_json}")
            except ValueError:
                log("Não foi possível converter a resposta da API para JSON.")

    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API: {e}")
