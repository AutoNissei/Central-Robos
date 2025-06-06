import os
import logging
import pyodbc
import requests
from datetime import datetime

from dotenv import load_dotenv


def get_logger(nome_robo: str):
    log_dir = os.path.join("logs", f"logs_{nome_robo}")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(f"logger_{nome_robo}")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger.info


log = get_logger("cadastro_de_prescritor")
load_dotenv()
central_db_config = {
    "server": os.getenv("CENTRAL_DB_SERVER"),
    "database": os.getenv("CENTRAL_DB_DATABASE"),
    "username": os.getenv("CENTRAL_DB_USER"),
    "password": os.getenv("CENTRAL_DB_PASS")
}
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


def cadastrar_prescritor(uf, crm, tipo_cr, token_api):
    mensagem = ""

    if tipo_cr.upper() != "CRM":
        mensagem = f"Os cadastros de {tipo_cr} precisam ser feitos manualmente, chamado encaminhado para análise"
        log(mensagem)
        return mensagem, tipo_cr

    try:
        # === CONSULTA API DATAHUB ===
        consulta_url = "https://datahub-api.nisseilabs.com.br/clientes/medico"
        params = {
            "crm_id": crm,
            "uf": uf.upper()
        }
        headers = {
            "Authorization": f"Bearer {token_api}"
        }

        response = requests.get(consulta_url, params=params, headers=headers)
        if response.status_code != 200:
            log(f"Erro ao consultar API do médico. Status: {response.status_code} - {response.text}")
            return "Erro ao consultar API de médicos.", tipo_cr

        medico_data = response.json()

        if medico_data["status"] != "Ativo":
            mensagem = f"Médico com CRM {crm} não está ativo na UF {uf.upper()}."
            log(mensagem)
            return mensagem, tipo_cr

        nome_medico = medico_data["nome"].upper()

    except Exception as e:
        log(f"Erro ao consultar dados do médico: {e}")
        return "Falha ao buscar médico na API.", tipo_cr

    try:
        conn = conectar_central()
        if not conn:
            return "Erro na conexão com o banco central.", tipo_cr

        cursor = conn.cursor()

        # Verifica se já existe
        cursor.execute("SELECT COUNT(*) FROM MEDICOS WHERE CR = ? AND UF = ?", (crm, uf.upper()))
        resultado = cursor.fetchone()

        if resultado[0] > 0:
            cursor.execute("UPDATE MEDICOS SET NOME = ? WHERE CR = ? AND UF = ?", (nome_medico, crm, uf.upper()))
            conn.commit()
            mensagem = f"Médico {nome_medico}, CRM {crm}, já cadastrado na UF {uf.upper()}"
        else:
            insert_query = "INSERT INTO MEDICOS (TAB_MASTER_ORIGEM, NOME, CODIGO_CR, CR, UF, SITUACAO_INSCRICAO) VALUES (?, ?, 1, ?, ?, 19)"
            cursor.execute(insert_query, (530427, nome_medico, crm, uf.upper()))
            conn.commit()

            #atualizar a REG_MASTER_ORIGEM
            cursor.execute("SELECT MEDICO FROM MEDICOS WHERE CR = ? AND UF = ?", (crm, uf.upper()))
            resultado = cursor.fetchone()
            if resultado:
                medico_id = resultado[0]
                cursor.execute("UPDATE MEDICOS SET REG_MASTER_ORIGEM = ? WHERE MEDICO = ?",(medico_id, medico_id))
                conn.commit()

            mensagem = f"Médico {nome_medico}, CRM {crm}, cadastrado com sucesso!"

        log(mensagem)
        return mensagem, tipo_cr

    except Exception as e:
        log(f"Erro ao cadastrar médico no banco: {e}")
        return "Erro ao cadastrar médico na central, por favor re-abra o chamado.", tipo_cr

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def interagir_chamado(cod_chamado, token_desk, mensagem, tipo_cr):
    descricao = f"{mensagem}\n"
    cod_status = "0000006" if tipo_cr.upper() != "CRM" else "0000002"
    data_interacao = datetime.now().strftime("%d-%m-%Y")

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
        "Authorization": token_desk,
        "Content-Type": "application/json"
    }

    try:
        response = requests.put("https://api.desk.ms/ChamadosSuporte/interagir", json=payload, headers=headers)
        if response.status_code == 200:
            if cod_status == "0000006":
                log(f"✅Chamado {cod_chamado} encaminhado para análise. \n")

            if cod_status == "0000002":
                log(f"✅Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"❌ Erro ao interagir no chamado {cod_chamado}. Código: {response.status_code}")
            log(response.text)
    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API Desk.ms: {e}")
