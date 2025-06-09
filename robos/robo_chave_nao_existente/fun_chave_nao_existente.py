import pyodbc
import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import logging



import sys, os

def resource_path(relative_path):
    return os.path.join(getattr(sys, '_MEIPASS', os.path.abspath(".")), relative_path)

# Carrega o .env com o caminho correto
dotenv_path = resource_path("robos/robo_chave_nao_existente/.env")  # ou o outro .env

load_dotenv(dotenv_path)

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



log = get_logger("chave_nao_existente")

load_dotenv()

awayson_db_config = {
    "server": os.getenv("AWAYSON_DB_SERVER"),
    "database": os.getenv("AWAYSON_DB_DATABASE"),
    "username": os.getenv("AWAYSON_DB_USER"),
    "password": os.getenv("AWAYSON_DB_PASS")
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

def conectar_awayson():
    """Estabelece conexão com o banco de dados awayson e retorna a conexão."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={awayson_db_config['server']};"
            f"DATABASE={awayson_db_config['database']};"
            f"UID={awayson_db_config['username']};"
            f"PWD={awayson_db_config['password']}"
        )
        return conn

    except Exception as e:
        log(f"Erro ao conectar ao banco awayson: {e}")
        return None


def integrar_notas_filial(nf_compra, pedido_compra, num_filial):
    try:
        conn_filial = conectar_filial(num_filial)
        cursor = conn_filial.cursor()
        sql = f"""
DECLARE @NF_COMPRA     VARCHAR(15) = {nf_compra}
DECLARE @PEDIDO_COMPRA VARCHAR(15) = {pedido_compra}

SET NOCOUNT ON



---  Exclui os dados no banco local 
--- ...
IF EXISTS (SELECT TOP 1 1 FROM NF_COMPRA WHERE NF_COMPRA = @NF_COMPRA) 
   BEGIN   DELETE		  FROM NF_COMPRA WHERE NF_COMPRA = @NF_COMPRA END
IF EXISTS (SELECT TOP 1 1 FROM PEDIDOS_COMPRAS WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA) 
   BEGIN   DELETE		  FROM PEDIDOS_COMPRAS WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA END
IF EXISTS (SELECT TOP 1 1 FROM PEDIDOS_COMPRAS_PRODUTOS WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA) 
   BEGIN   DELETE		  FROM PEDIDOS_COMPRAS_PRODUTOS WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA END

--- Insere os dados na Tabela
--- ...
INSERT INTO [NF_COMPRA] 
		   ([NF_COMPRA]
		   ,[FORMULARIO_ORIGEM]
		   ,[TAB_MASTER_ORIGEM]
		   ,[REG_MASTER_ORIGEM]
		   ,[REG_LOG_INCLUSAO]
		   ,[ROWVERSION]
		   ,[EMPRESA]
		   ,[ENTIDADE]
		   ,[MOVIMENTO]
		   ,[NF_ESPECIE]
		   ,[NF_SERIE]
		   ,[NF_NUMERO]
		   ,[CHAVE_NFE]
		   ,[PEDIDO_COMPRA]
		   ,[RECEBIMENTO]
		   ,[TOTAL_GERAL]
		   ,[PROCESSAR]
		   ,[STATUS_VALIDACAO_COMERCIAL])
EXEC ('SELECT [NF_COMPRA]
		     ,[FORMULARIO_ORIGEM]
		     ,[TAB_MASTER_ORIGEM]
		     ,[REG_MASTER_ORIGEM]
		     ,111 AS [REG_LOG_INCLUSAO]
		     ,[ROWVERSION]
		     ,[EMPRESA]
		     ,[ENTIDADE]
		     ,[MOVIMENTO]
		     ,[NF_ESPECIE]
		     ,[NF_SERIE]
		     ,[NF_NUMERO]
		     ,[CHAVE_NFE]
		     ,[PEDIDO_COMPRA]
		     ,[RECEBIMENTO]
		     ,[TOTAL_GERAL]
		     ,[PROCESSAR]
		     ,[STATUS_VALIDACAO_COMERCIAL]
	     FROM NF_COMPRA
		WHERE NF_COMPRA = ''' + @NF_COMPRA + '''') AT [RETAGUARDA]

INSERT INTO [PEDIDOS_COMPRAS] 
	       ([PEDIDO_COMPRA]
	       ,[FORMULARIO_ORIGEM]
	       ,[TAB_MASTER_ORIGEM]
	       ,[REG_MASTER_ORIGEM]
	       ,[REG_LOG_INCLUSAO]
	       ,[ROWVERSION]
	       ,[ENTIDADE]
	       ,[EMPRESA]
	       ,[DATA_HORA]
	       ,[SUGESTAO_COMPRA]
	       ,[CICLO_ELETRONICO])
EXEC ('SELECT [PEDIDO_COMPRA]
,[FORMULARIO_ORIGEM]
,[TAB_MASTER_ORIGEM]
,[REG_MASTER_ORIGEM]
,111 AS [REG_LOG_INCLUSAO]
,[ROWVERSION]
,[ENTIDADE]
,[EMPRESA]
,[DATA_HORA]
,[SUGESTAO_COMPRA]
,[CICLO_ELETRONICO]
FROM PEDIDOS_COMPRAS
WHERE PEDIDO_COMPRA = ''' + @PEDIDO_COMPRA + '''') AT [RETAGUARDA]

SET IDENTITY_INSERT PEDIDOS_COMPRAS_PRODUTOS ON
INSERT INTO [PEDIDOS_COMPRAS_PRODUTOS] 
([PEDIDO_COMPRA_PRODUTO]
,[FORMULARIO_ORIGEM]
,[TAB_MASTER_ORIGEM]
,[REG_MASTER_ORIGEM]
,[REG_LOG_INCLUSAO]
,[ROWVERSION]
,[PEDIDO_COMPRA]
,[REFERENCIA]
,[PRODUTO]
,[QUANTIDADE]
,[QUANTIDADE_EMBALAGEM]
,[QUANTIDADE_ESTOQUE])
EXEC ('SELECT [PEDIDO_COMPRA_PRODUTO]
,[FORMULARIO_ORIGEM]
,[TAB_MASTER_ORIGEM]
,[REG_MASTER_ORIGEM]
,111 AS [REG_LOG_INCLUSAO]
,[ROWVERSION]
,[PEDIDO_COMPRA]
,[REFERENCIA]
,[PRODUTO]
,[QUANTIDADE]
,[QUANTIDADE_EMBALAGEM]
,[QUANTIDADE_ESTOQUE]
FROM PEDIDOS_COMPRAS_PRODUTOS
WHERE PEDIDO_COMPRA = ''' + @PEDIDO_COMPRA + '''') AT [RETAGUARDA]
SET IDENTITY_INSERT PEDIDOS_COMPRAS_PRODUTOS OFF

SET NOCOUNT OFF

--- Consulta final
--- ...
SELECT A.NF_COMPRA
,A.EMPRESA
,A.ENTIDADE AS FORNECEDOR
,CONVERT(VARCHAR, A.MOVIMENTO, 103) AS MOVIMENTO
,A.NF_SERIE
,A.NF_NUMERO
,B.PEDIDO_COMPRA
,C.PRODUTO
,D.DESCRICAO
,C.QUANTIDADE
FROM NF_COMPRA AS A
JOIN PEDIDOS_COMPRAS AS B ON A.PEDIDO_COMPRA = B.PEDIDO_COMPRA
JOIN PEDIDOS_COMPRAS_PRODUTOS AS C ON B.PEDIDO_COMPRA = C.PEDIDO_COMPRA
JOIN PRODUTOS AS D ON C.PRODUTO = D.PRODUTO
WHERE A.NF_COMPRA = @NF_COMPRA
AND B.PEDIDO_COMPRA = @PEDIDO_COMPRA
                """
        cursor.execute(sql)
        conn_filial.commit()

        cursor.execute("SELECT TOP 1 1 FROM NF_COMPRA WHERE NF_COMPRA = ?", nf_compra)
        resultado = cursor.fetchone()
        if resultado:
            return True
        else:
            return False
    except Exception as e:
        log(f"Erro ao integrar a nota na filial : {e}")
    finally:
        if cursor:
            cursor.close()
        if conn_filial:
            conn_filial.close()


def consultar_notas_central(chaves, num_filial):
    try:
        conn = conectar_awayson()
        if conn is None:
            return []

        cursor = conn.cursor()

        notas_integradas = []
        notas_nao_central = []
        notas_sem_pedido = []
        notas_outra_filial = []
        notas_nao_integradas = []

        for chave in chaves:
            cursor.execute("SELECT NF_COMPRA, PEDIDO_COMPRA, EMPRESA FROM NF_COMPRA WHERE CHAVE_NFE = ?", (chave,))
            resultado = cursor.fetchone()

            if resultado:
                nf_compra, pedido_compra, empresa = resultado
                nota_info = {"CHAVE": chave, "EMPRESA": empresa}

                if pedido_compra is None:
                    notas_sem_pedido.append(chave)
                    continue

                if nota_info["EMPRESA"] == num_filial:
                    if integrar_notas_filial(nf_compra, pedido_compra, num_filial):
                        notas_integradas.append(chave)
                    else:
                        notas_nao_integradas.append(chave)

                else:
                    notas_outra_filial.append(nota_info)
            else:
                notas_nao_central.append(chave)

        return notas_integradas, notas_nao_central, notas_sem_pedido, notas_outra_filial, notas_nao_integradas

    except Exception as e:
        log(f"Erro ao consultar notas na central: {e}")
        return [], [], [], [], []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def interagir_chamado(cod_chamado, token, notas_integradas, notas_nao_central, notas_sem_pedido, notas_outra_filial,
                      notas_nao_integradas):
    descricao = "Resumo da Integração de Notas\n\n"

    if notas_integradas:
        descricao += "*Notas Integradas na Filial:*\n" + "\n".join(notas_integradas) + "\n\n"

    if notas_nao_integradas:
        descricao += "*Não foi possível integrar as seguintes notas:*\n" + "\n".join(notas_nao_integradas) + "\n\n"
        descricao += "Favor abrir um novo chamado para essas notas.\n\n"

    if notas_sem_pedido:
        descricao += "*Notas sem Pedido de Compra:*\n" + "\n".join(notas_sem_pedido) + "\n\n"
        descricao += "Favor abrir um chamado com o assunto CSN - PEDIDO COMPLEMENTAR para as notas sem pedido de compra.\n\n"

    if notas_nao_central:
        descricao += "*Notas não encontradas na Central:*\n" + "\n".join(notas_nao_central) + "\n\n"
        descricao += "Chamado encaminhado para análise.\n\n"

    if notas_outra_filial:
        descricao += "*As seguintes notas não pertencem a esta loja:*\n"
        for nota in notas_outra_filial:
            descricao += f"{nota['CHAVE']} --  FILIAL {nota['EMPRESA']}\n"
        descricao += "\n"

    if notas_nao_central:
        cod_status = "0000006"
    else:
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
            if cod_status == "0000006":
                log(f"Chamado {cod_chamado} encaminhado para análise. \n")

            if cod_status == "0000002":
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
