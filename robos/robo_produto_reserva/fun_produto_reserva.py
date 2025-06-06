import os
import logging
from datetime import datetime
import requests
from dotenv import load_dotenv
import pyodbc





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



log = get_logger("produto_reserva")


load_dotenv()

awayson_db_config = {
    "server": os.getenv("AWAYSON_DB_SERVER"),
    "database": os.getenv("AWAYSON_DB_DATABASE"),
    "username": os.getenv("AWAYSON_DB_USER"),
    "password": os.getenv("AWAYSON_DB_PASS")
}


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


def consultar_estoque(filial, produto):
    try:
        conn = conectar_awayson()
        cursor = conn.cursor()

        # Converta para int

        cursor.execute('''SELECT ESTOQUE_SALDO FROM ESTOQUE_ATUAL WHERE CENTRO_ESTOQUE = ? AND PRODUTO = ?''',
                       (filial, produto))

        resultado = cursor.fetchone()
        if resultado and resultado[0] is not None:
            estoque_atual = int(resultado[0])
            log(f"Estoque atual da loja para o produto {produto}: {estoque_atual}")
            return estoque_atual
        else:
            log("Estoque não encontrado.")
            return 0

    except Exception as e:
        log(f"Erro ao consultar estoque: {e}")
        return None







def criar_remanejamento(loja_origem, loja_destino, produto, estoque_produto):
    mensagem = None
    try:
        conn = conectar_awayson()  # função que retorna conexão com banco central
        cursor = conn.cursor()

        insert_sql = """
        INSERT INTO REMANEJAMENTOS_ESTOQUES_LOJAS (
            FORMULARIO_ORIGEM, TAB_MASTER_ORIGEM, REG_LOG_INCLUSAO, DATA_HORA, USUARIO_LOGADO,
            MOVIMENTO, TIPO_REMANEJAMENTO_LOJA, APENAS_CONTROLADOS,
            MENOS_CONTROLADOS, PERCENTUAL_EXCESSO, MINIMO_ORIGEM,
            MINIMO_DESTINO, REVERSA, CONSIDERAR_INATIVOS_COMO_EXCESSO,
            QUEBRA_POR_VOLUMES, EMPRESA
        )
        VALUES (?, ?, ?, GETDATE(), ?, CONVERT(DATE, GETDATE()), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        SELECT SCOPE_IDENTITY();
        """

        valores = (
            302379, 301856, 0, 49479, 4, 'N', 1, 100, 'S', 'S', 'N', 'N', 'N', loja_origem
        )

        cursor.execute(insert_sql, valores)
        while cursor.nextset():
            try:
                row = cursor.fetchone()
                if row:
                    remanejamento_id = int(row[0])
                    break

            except:
                continue
        else:
            raise Exception("❌ Não foi possível capturar o ID do escopo via SCOPE_IDENTITY()")
        conn.commit()
        if remanejamento_id:
            cursor.execute(
                "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET REG_MASTER_ORIGEM = ? WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
                (remanejamento_id, remanejamento_id))
            conn.commit()
        log('1.1 Success - inclusão na tabela master')

        insert_origens_sql = '''INSERT INTO REMANEJAMENTOS_ESTOQUES_LOJAS_ORIGENS(
            FORMULARIO_ORIGEM, TAB_MASTER_ORIGEM, REG_MASTER_ORIGEM, REMANEJAMENTO_ESTOQUE_LOJA,
            EMPRESA, EXCESSO, ORDEM, PROCESSADO_EXCESSO, DATA_HORA)

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CONVERT(DATE, GETDATE()));'''

        valores_origens = (302379, 301856, remanejamento_id, remanejamento_id, loja_origem, estoque_produto, 1, 'S')

        cursor.execute(insert_origens_sql, valores_origens)
        conn.commit()
        log('1.2 Success - inclusão na tabela origens')

        insert_destino_sql = '''INSERT INTO REMANEJAMENTOS_ESTOQUES_LOJAS_DESTINOS(
            FORMULARIO_ORIGEM, TAB_MASTER_ORIGEM, REG_MASTER_ORIGEM, REMANEJAMENTO_ESTOQUE_LOJA, EMPRESA, DATA_HORA)

            VALUES (?, ?, ?, ?, ?, CONVERT(DATE, GETDATE()));
        '''

        valores_destinos = (302379, 301856, remanejamento_id, remanejamento_id, loja_destino)

        cursor.execute(insert_destino_sql, valores_destinos)
        conn.commit()
        log('1.3 Success - inclusão na tabela destinos')

        insert_produtos_sql = '''INSERT INTO REMANEJAMENTOS_ESTOQUES_LOJAS_PRODUTOS (
            FORMULARIO_ORIGEM, TAB_MASTER_ORIGEM, REG_MASTER_ORIGEM, REMANEJAMENTO_ESTOQUE_LOJA, PRODUTO, TIPO, DATA_HORA)

            VALUES (?, ?, ?, ?, ?, ?, CONVERT(DATE, GETDATE()));
            '''

        valores_produtos = (302379, 301856, remanejamento_id, remanejamento_id, produto, '+')
        cursor.execute(insert_produtos_sql, valores_produtos)
        conn.commit()
        log('1.4 Success - inclusão na tabela produtos')

        # === Etapas de processamento ===

        # 4. Marcar PROCESSAR_NECESSIDADE
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET PROCESSAR_NECESSIDADE = 'S' WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()

        # 5. Executar procedures de necessidade
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_PRODUTOS_FILTROS ?", remanejamento_id)
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_EXCESSOS ?", remanejamento_id)
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_NECESSIDADES ?", remanejamento_id)
        conn.commit()

        # 6. Rateio
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_RATEIOS ?, ?", remanejamento_id, loja_origem)
        conn.commit()

        # 7. Romaneios
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_ROMANEIOS ?, ?", remanejamento_id, loja_origem)
        conn.commit()

        # 8. Envio para DataHub (opcional)
        cursor.execute("EXEC USP_DATAHUB_MONTA_JSON_ENVIO_ESTOQUE_TRANSFERENCIAS ?, 2", remanejamento_id)
        conn.commit()

        # 9. Atualização final
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET REG_LOG_INCLUSAO = 0 WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET PROCESSAR_NECESSIDADE = 'N' WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()
        log('2 Success - Processar necessidades executado com sucesso')

        # PROCESSAR RATEIO
        # Atualiza flag
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET PROCESSAR_RATEIO = 'S' WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()

        # Limpa tabela de quantidades
        cursor.execute("DELETE FROM REMANEJAMENTOS_ESTOQUES_LOJAS_QUANTIDADES WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
                       remanejamento_id)
        conn.commit()

        # Zera dados da tabela de necessidades
        cursor.execute("""
            UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS_NECESSIDADES
            SET QUANTIDADE_REMANEJADA = 0,
                QUANTIDADE_SUGERIDA_UNITARIA = 0
            WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?
        """, remanejamento_id)
        conn.commit()

        # Seta PROCESSADO_EXCESSO como 'N' para todas as origens
        cursor.execute("""
            UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS_ORIGENS
            SET PROCESSADO_EXCESSO = 'N'
            WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?
        """, remanejamento_id)
        conn.commit()

        # Consulta as lojas de origem
        cursor.execute("""
            SELECT EMPRESA FROM REMANEJAMENTOS_ESTOQUES_LOJAS_ORIGENS
            WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?
              AND PROCESSADO_EXCESSO = 'N'
              AND EXCESSO > 0
        """, remanejamento_id)
        lojas_origem = cursor.fetchall()

        # Executa a procedure para cada loja de origem
        for (empresa_origem,) in lojas_origem:
            cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_RATEIOS ?, ?", remanejamento_id, empresa_origem)
            conn.commit()

        # Atualiza a flag final
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET PROCESSAR_RATEIO = 'N' WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()
        log('3 Success - Processar Rateio executado com sucesso')

        # Atualiza PROCESSAR_ROMANEIOS
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET PROCESSAR_ROMANEIOS = 'S' WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()

        # Executa o processamento do romaneio
        cursor.execute("EXEC PROCESSAMENTO_REMANEJAMENTO_LOJAS_ROMANEIOS ?, ?", remanejamento_id, loja_origem)
        conn.commit()

        # Monta JSON para envio ao DataHub
        cursor.execute("EXEC USP_DATAHUB_MONTA_JSON_ENVIO_ESTOQUE_TRANSFERENCIAS ?, 2", remanejamento_id)
        conn.commit()

        # Força atualização do registro
        cursor.execute(
            "UPDATE REMANEJAMENTOS_ESTOQUES_LOJAS SET REG_LOG_INCLUSAO = 0 WHERE REMANEJAMENTO_ESTOQUE_LOJA = ?",
            remanejamento_id)
        conn.commit()
        log('3 Success - Processar Romaneios executado com sucesso')

        log(f"✅ Remanejamento criado com ID {remanejamento_id} para produto {produto} da loja {loja_origem} para {loja_destino}")

        capturar_romaneio_script = '''
        DECLARE @REMANEJAMENTO_ESTOQUE_LOJA NUMERIC = ?

        DECLARE @ESTOQUE_TRANSFERENCIAS TABLE ( ESTOQUE_TRANSFERENCIA NUMERIC(15) PRIMARY KEY )

        INSERT INTO @ESTOQUE_TRANSFERENCIAS ( ESTOQUE_TRANSFERENCIA )
        SELECT A.ESTOQUE_TRANSFERENCIA
        FROM ESTOQUE_TRANSFERENCIAS A WITH(NOLOCK)
        WHERE A.REMANEJAMENTO_ESTOQUE_LOJA = @REMANEJAMENTO_ESTOQUE_LOJA

        DECLARE @ESTOQUE_TRANSFERENCIAS_002 TABLE ( ESTOQUE_TRANSFERENCIA NUMERIC(15) PRIMARY KEY,
                                                    TOTAL_UNIDADES NUMERIC(15,2) )

        INSERT INTO @ESTOQUE_TRANSFERENCIAS_002 
        SELECT A.ESTOQUE_TRANSFERENCIA,
               SUM(B.QUANTIDADE_UNITARIA)
        FROM @ESTOQUE_TRANSFERENCIAS A 
        JOIN ESTOQUE_TRANSFERENCIAS_PRODUTOS B WITH(NOLOCK) ON B.ESTOQUE_TRANSFERENCIA = A.ESTOQUE_TRANSFERENCIA
        GROUP BY A.ESTOQUE_TRANSFERENCIA

        SELECT A.ESTOQUE_TRANSFERENCIA
        FROM ESTOQUE_TRANSFERENCIAS A WITH(NOLOCK)
        JOIN CENTROS_ESTOQUE C WITH(NOLOCK) ON C.OBJETO_CONTROLE = A.CENTRO_ESTOQUE_ORIGEM
        JOIN CENTROS_ESTOQUE D WITH(NOLOCK) ON D.OBJETO_CONTROLE = A.CENTRO_ESTOQUE_DESTINO
        JOIN @ESTOQUE_TRANSFERENCIAS_002 X ON X.ESTOQUE_TRANSFERENCIA = A.ESTOQUE_TRANSFERENCIA
        ORDER BY A.EMPRESA, A.CENTRO_ESTOQUE_DESTINO
        '''

        # Executa o script
        cursor.execute(capturar_romaneio_script, remanejamento_id)

        # Ignora todos os resultsets intermediários até chegar no SELECT final
        while cursor.description is None:
            if not cursor.nextset():
                break  # nenhum outro conjunto de resultados
        # Agora captura o resultado real
        romaneio = cursor.fetchone()

        if romaneio:
            romaneio = romaneio[0]
            log(f"Romaneio gerado: {romaneio}")
        else:
            log("Falha ao capturar o romaneio")

        mensagem = f'Segue número do romaneio gerado com sucesso: {romaneio}'


    except Exception as e:
        conn.rollback()
        log(f"❌ Erro ao criar remanejamento: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return mensagem


def interagir_chamado(cod_chamado, token_desk, mensagem):
    if mensagem:
        descricao = f"{mensagem}\n"
    else:
        descricao = "Ocorreu um erro na geração do romaneio, por favor verifique as informações e abra um novo chamado."
    data_interacao = datetime.now().strftime("%d-%m-%Y")

    payload = {
        "Chave": cod_chamado,
        "TChamado": {
            "CodFormaAtendimento": "1",
            "CodStatus": "0000002",
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
            log(f"✅Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"❌ Erro ao interagir no chamado {cod_chamado}. Código: {response.status_code}")
            log(response.text)
    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API Desk.ms: {e}")
