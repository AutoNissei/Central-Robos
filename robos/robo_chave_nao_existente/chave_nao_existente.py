import requests
import re
from . import fun_chave_nao_existente as fun


def run(ativo):
    log = fun.get_logger("chave_nao_existente")

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
        response = requests.post(url_auth, json=payload_auth, headers=headers_auth)
        response.raise_for_status()
        token = response.json().get("access_token")

        if not token:
            log("❌ Token de autenticação não retornado.")
            return

        log("✅ Autenticação realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro durante a autenticação: {e}")
        return

    # === 2. Requisição para listar chamados ===
    url_chamados = "https://api.desk.ms/ChamadosSuporte/lista"
    headers_chamados = {"Authorization": f"{token}"}
    payload_chamados = {
        "Pesquisa": "CSN - CHAVE NAO EXISTENTE NO BANCO DE DADOS",
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
            "_6313": "on"
        },
        "Ordem": [
            {
                "Coluna": "Chave",
                "Direcao": "true"
            }
        ]
    }

    try:
        response = requests.post(url_chamados, json=payload_chamados, headers=headers_chamados)
        response.raise_for_status()
        chamados = response.json().get("root", [])

        if not chamados:
            log("ℹ️ Nenhum chamado encontrado.")
            return

        for chamado in chamados:
            cod_chamado = chamado["CodChamado"]
            descricao = chamado["Descricao"]
            nome_usuario = chamado["NomeUsuario"]

            log(f"Chamado: {cod_chamado}")

            # === Extrair chaves de NF ===
            chaves_nf = re.findall(r"\b\d{44}\b", descricao)
            if not chaves_nf:
                log("⚠️ Nenhuma chave de NF encontrada na descrição.")
                continue
            log(f"Chaves encontradas: {', '.join(chaves_nf)}")

            # === Extrair número da filial ===
            match = re.search(r"\d+", nome_usuario)
            if not match:
                log("⚠️ Não foi possível identificar a filial.")
                continue
            num_filial = int(match.group())
            log(f"Filial identificada: {num_filial}")

            try:
                # === Consultar notas na central ===
                notas_integradas, notas_nao_central, notas_sem_pedido, notas_outra_filial, notas_nao_integradas = fun.consultar_notas_central(
                    chaves_nf, num_filial)

                # Interagir no chamado
                fun.interagir_chamado(cod_chamado, token, notas_integradas, notas_nao_central, notas_sem_pedido,
                                      notas_outra_filial, notas_nao_integradas)

            except Exception as e:
                log(f"❌ Erro ao processar chamado {cod_chamado}: {e}")

    except requests.RequestException as e:
        log(f"❌ Erro na requisição de chamados: {e}")

    except Exception as e:
        log(f"❌ Erro inesperado: {e}")
