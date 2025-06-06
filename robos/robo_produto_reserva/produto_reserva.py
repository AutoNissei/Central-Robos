import requests
import re
from . import fun_produto_reserva as fun


def run(ativo):
    log = fun.get_logger("produto_reserva")

    # === 1. Autenticação na API Desk.ms ===
    url_auth = "https://api.desk.ms/Login/autenticar"
    headers_auth = {
        "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
        "JsonPath": "true"
    }
    payload_auth = {
        "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
    }

    try:
        response_auth = requests.post(url_auth, json=payload_auth, headers=headers_auth)
        response_auth.raise_for_status()

        token = response_auth.json().get("access_token")
        if not token:
            log("❌ Token não retornado na autenticação. Encerrando execução.")
            return

        log("✅ Autenticação realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro durante a autenticação com a API Desk.ms: {e}")
        return

    # === 2. Listagem de Chamados ===
    url_chamados = "https://api.desk.ms/ChamadosSuporte/lista"
    headers_chamados = {"Authorization": token}
    payload_chamados = {
        "Pesquisa": "CSN - PRODUTO EM RESERVA REMANEJAMENTO",
        "Tatual": "",
        "Ativo": ativo,
        "StatusSLA": "",
        "Colunas": {
            "Chave": "on",
            "CodChamado": "on",
            "NomePrioridade": "on",
            "DataCriacao": "on",
            "HoraCriacao": "on",
            "DataFinalizacao": "on",
            "HoraFinalizacao": "on",
            "DataAlteracao": "on",
            "HoraAlteracao": "on",
            "NomeStatus": "on",
            "Assunto": "on",
            "Descricao": "on",
            "ChaveUsuario": "on",
            "NomeUsuario": "on",
            "SobrenomeUsuario": "on",
            "NomeCompletoSolicitante": "on",
            "SolicitanteEmail": "on",
            "NomeOperador": "on",
            "SobrenomeOperador": "on",
            "TotalAcoes": "on",
            "TotalAnexos": "on",
            "Sla": "on",
            "CodGrupo": "on",
            "NomeGrupo": "on",
            "CodSolicitacao": "on",
            "CodSubCategoria": "on",
            "CodTipoOcorrencia": "on",
            "CodCategoriaTipo": "on",
            "CodPrioridadeAtual": "on",
            "CodStatusAtual": "on",
            "_262125": "on",  # Produto
            "_86425": "on",   # Filial Origem
            "_86427": "on"    # Filial Destino
        },
        "Ordem": [
            {
                "Coluna": "Chave",
                "Direcao": "true"
            }
        ]
    }

    try:
        response_chamados = requests.post(url_chamados, json=payload_chamados, headers=headers_chamados)
        response_chamados.raise_for_status()

        chamados = response_chamados.json().get("root", [])
        if not chamados:
            log("ℹ️ Nenhum chamado encontrado com os critérios informados.")
            return

        for chamado in chamados:
            cod_chamado = chamado["CodChamado"]
            produto = chamado.get("_262125")
            filial_origem = chamado.get("_86425")
            filial_destino = chamado.get("_86427")
            nome_usuario = chamado.get("NomeUsuario", "")

            log(f"Chamado: {cod_chamado}")
            log(f"➡️ Produto: {produto} | Origem: {filial_origem} | Destino: {filial_destino}")

            # === Identificar número da filial a partir do nome do usuário
            match_filial = re.search(r"\d+", nome_usuario)
            if not match_filial:
                log("⚠️ Não foi possível identificar a filial a partir do nome do usuário. Pulando chamado.")
                continue
            num_filial = int(match_filial.group())
            log(f"Filial base identificada: {num_filial}")

            try:
                # === Consulta de estoque ===
                estoque_produto = fun.consultar_estoque(num_filial, produto)
                if estoque_produto is None:
                    log(f"⚠️ Estoque não localizado para o produto {produto}. Pulando chamado.")
                    continue

                # === Criação do remanejamento ===
                mensagem = fun.criar_remanejamento(
                    loja_origem=filial_origem,
                    loja_destino=filial_destino,
                    produto=produto,
                    estoque_produto=estoque_produto
                )

                # === Interagir no chamado ===
                fun.interagir_chamado(cod_chamado, token, mensagem)

            except Exception as e:
                log(f"❌ Erro ao processar chamado {cod_chamado}: {e}")

    except Exception as e:
        log(f"❌ Erro ao consultar chamados: {e}")

