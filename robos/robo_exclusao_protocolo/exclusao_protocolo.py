import requests
import re
from . import fun_exclusao_protocolo as fun

def run(ativo):
    log = fun.get_logger("exclusao_protocolo")

    # === 1. Autenticação ===
    try:
        url_auth = "https://api.desk.ms/Login/autenticar"
        headers_auth = {
            "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
            "JsonPath": "true"
        }
        payload_auth = {
            "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
        }

        response = requests.post(url_auth, json=payload_auth, headers=headers_auth)
        response.raise_for_status()

        token = response.json().get("access_token")
        if not token:
            log("❌ Token não retornado na autenticação.")
            return
        log("✅ Autenticação realizada com sucesso.")

    except Exception as e:
        log(f"❌ Erro durante a autenticação: {e}")
        return

    # === 2. Listar chamados ===
    url_chamados = "https://api.desk.ms/ChamadosSuporte/lista"
    headers_chamados = {"Authorization": token}
    payload_chamados = {
        "Pesquisa": "CSN - EXCLUSAO PROTOCOLO DE NÃO RECEBIMENTO",
        "Tatual": "",
        "Ativo": ativo,
        "StatusSLA": "",
        "Colunas": {
            "Chave": "on", "CodChamado": "on", "Descricao": "on",
            "NomeUsuario": "on", "_6313": "on"
        },
        "Ordem": [{"Coluna": "Chave", "Direcao": "true"}]
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

            # === Extrair chaves da nota ===
            chaves_nf = re.findall(r"\b\d{44}\b", descricao)
            if not chaves_nf:
                log("⚠️ Nenhuma chave de nota fiscal encontrada na descrição.")
                continue

            # === Extrair número da filial ===
            match = re.search(r"\d+", nome_usuario)
            if not match:
                log("⚠️ Não foi possível identificar a filial.")
                continue

            num_filial = int(match.group())
            log(f"Filial identificada: {num_filial}")
            log(f"Chaves encontradas: {', '.join(chaves_nf)}")

            # === Processar exclusão de protocolos ===
            protocolos_nao_encontrados_central = fun.excluir_protocolo_central(chaves_nf)
            protocolos_nao_encontrados = fun.excluir_protocolo_filial(chaves_nf, num_filial, protocolos_nao_encontrados_central)

            # === Interagir no chamado ===
            fun.interagir_chamado(cod_chamado, token, protocolos_nao_encontrados)

    except requests.RequestException as e:
        log(f"❌ Erro de requisição ao listar chamados: {e}")
    except Exception as e:
        log(f"❌ Erro inesperado durante o processamento: {e}")
