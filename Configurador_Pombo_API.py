from __future__ import annotations
import json
import logging
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PureWindowsPath
from typing import Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
import requests
from psycopg.rows import dict_row
# ============================================================
# LOG
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("pombo_api")
IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# ============================================================
# DATACLASSES
# ============================================================
@dataclass(frozen=True)
class Fonte:
    id: int
    nome: str
    id_referencia: int
@dataclass(frozen=True)
class ConfigAPI:
    id: int
    nome_api: str
    metodo_http: str
    endpoint_base: str
    rota_recurso: str
    tipo_autenticacao: str | None
    usuario_api: str | None
    senha_api: str | None
    token_api: str | None
    api_key: str | None
    api_key_header: str | None
    headers_json: dict[str, Any]
    parametros_json: dict[str, Any]
    timeout_segundos: int
    qtd_tentativas: int
    intervalo_retry_seg: int
    paginacao_ativa: bool
    tipo_paginacao: str | None
    parametro_pagina: str | None
    parametro_limite: str | None
    tamanho_pagina: int
    campo_dados: str
    campo_proxima_pagina: str | None
    nome_alias: str
    harmoniza: bool
    gravadata: bool
@dataclass(frozen=True)
class Entidade:
    id: int
    id_fonte: int
    tabela: str
    tipo_carga: str
    intervalo_minutos: int
    tamanho_lote: int
    ultima_execucao: datetime | None
    proxima_execucao: datetime | None
    carga_inicial_concluida: bool
    data_carga_inicial: datetime | None
    ultima_execucao_status: str | None
    ultimo_parquet: str | None
@dataclass(frozen=True)
class ConfigPombo:
    caminho: str
    tipo_destino: str
    usuario: str | None
    senha: str | None
# ============================================================
# CONEXÃO HUB CONTROLE
# ============================================================
def hub_connect() -> psycopg.Connection:
    obrigatorias = {
        "HUB_CONTROLE_HOST": os.getenv("HUB_CONTROLE_HOST"),
        "HUB_CONTROLE_USER": os.getenv("HUB_CONTROLE_USER"),
        "HUB_CONTROLE_PASSWORD": os.getenv("HUB_CONTROLE_PASSWORD"),
    }
    faltantes = [k for k, v in obrigatorias.items() if not v]
    if faltantes:
        raise RuntimeError(
            "Variaveis ausentes: " + ", ".join(faltantes)
        )
    return psycopg.connect(
        host=obrigatorias["HUB_CONTROLE_HOST"],
        port=int(os.getenv("HUB_CONTROLE_PORT", "54320")),
        dbname=os.getenv("HUB_CONTROLE_DB", "central"),
        user=obrigatorias["HUB_CONTROLE_USER"],
        password=obrigatorias["HUB_CONTROLE_PASSWORD"],
        row_factory=dict_row,
    )
# ============================================================
# CONFIGURAÇÕES
# ============================================================
def buscar_config_pombo(hub: psycopg.Connection) -> ConfigPombo:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                caminho,
                tipo_destino,
                usuario,
                senha
            FROM public.configuracao_pombo
            WHERE ativo = TRUE
            ORDER BY id
            LIMIT 1
        """)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Nenhuma configuracao do Pombo ativa.")
    return ConfigPombo(
        caminho=row["caminho"],
        tipo_destino=(row.get("tipo_destino") or "SMB").upper(),
        usuario=row.get("usuario"),
        senha=row.get("senha"),
    )
def buscar_fontes_api(hub: psycopg.Connection) -> list[Fonte]:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                nome_fonte,
                id_referencia
            FROM public.fontes
            WHERE ativo = TRUE
              AND UPPER(tipo_fonte) = 'API'
            ORDER BY ordem_execucao, id
        """)
        rows = cur.fetchall()
    return [
        Fonte(
            id=r["id"],
            nome=r["nome_fonte"],
            id_referencia=r["id_referencia"],
        )
        for r in rows
    ]
def buscar_config_api(
    hub: psycopg.Connection,
    id_referencia: int,
) -> ConfigAPI:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                nome_api,
                metodo_http,
                endpoint_base,
                rota_recurso,
                tipo_autenticacao,
                usuario_api,
                senha_api,
                token_api,
                api_key,
                api_key_header,
                headers_json,
                parametros_json,
                timeout_segundos,
                qtd_tentativas,
                intervalo_retry_seg,
                paginacao_ativa,
                tipo_paginacao,
                parametro_pagina,
                parametro_limite,
                tamanho_pagina,
                campo_dados,
                campo_proxima_pagina,
                nome_alias,
                harmoniza,
                COALESCE(gravadata, TRUE) AS gravadata
            FROM public.configuracao_api
            WHERE id = %s
              AND ativo = TRUE
        """, (id_referencia,))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"Configuracao API {id_referencia} nao encontrada."
        )
    return ConfigAPI(
        id=row["id"],
        nome_api=row["nome_api"],
        metodo_http=(row.get("metodo_http") or "GET").upper(),
        endpoint_base=(row["endpoint_base"] or "").rstrip("/"),
        rota_recurso="/" + (row["rota_recurso"] or "").lstrip("/"),
        tipo_autenticacao=row.get("tipo_autenticacao"),
        usuario_api=row.get("usuario_api"),
        senha_api=row.get("senha_api"),
        token_api=row.get("token_api"),
        api_key=row.get("api_key"),
        api_key_header=row.get("api_key_header"),
        headers_json=dict(row.get("headers_json") or {}),
        parametros_json=dict(row.get("parametros_json") or {}),
        timeout_segundos=int(row.get("timeout_segundos") or 60),
        qtd_tentativas=int(row.get("qtd_tentativas") or 3),
        intervalo_retry_seg=int(row.get("intervalo_retry_seg") or 5),
        paginacao_ativa=bool(row.get("paginacao_ativa")),
        tipo_paginacao=row.get("tipo_paginacao"),
        parametro_pagina=row.get("parametro_pagina"),
        parametro_limite=row.get("parametro_limite"),
        tamanho_pagina=int(row.get("tamanho_pagina") or 1000),
        campo_dados=row.get("campo_dados") or "resultados",
        campo_proxima_pagina=row.get("campo_proxima_pagina"),
        nome_alias=row.get("nome_alias") or "POSTOS",
        harmoniza=bool(row.get("harmoniza")),
        gravadata=bool(row.get("gravadata")),
    )
def buscar_entidades(
    hub: psycopg.Connection,
    id_fonte: int,
) -> list[Entidade]:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                id_fonte,
                nome_tabela,
                tipo_carga,
                intervalo_minutos,
                tamanho_lote,
                ultima_execucao,
                proxima_execucao,
                carga_inicial_concluida,
                data_carga_inicial,
                ultima_execucao_status,
                ultimo_parquet
            FROM public.configuracao_entidades
            WHERE id_fonte = %s
              AND ativo = TRUE
              AND (
                    proxima_execucao IS NULL
                    OR proxima_execucao <= CURRENT_TIMESTAMP
              )
            ORDER BY prioridade, id
        """, (id_fonte,))
        rows = cur.fetchall()
    return [
        Entidade(
            id=r["id"],
            id_fonte=r["id_fonte"],
            tabela=r["nome_tabela"],
            tipo_carga=(r.get("tipo_carga") or "INCREMENTAL").upper(),
            intervalo_minutos=int(r.get("intervalo_minutos") or 60),
            tamanho_lote=int(r.get("tamanho_lote") or 1000),
            ultima_execucao=r.get("ultima_execucao"),
            proxima_execucao=r.get("proxima_execucao"),
            carga_inicial_concluida=bool(
                r.get("carga_inicial_concluida")
            ),
            data_carga_inicial=r.get("data_carga_inicial"),
            ultima_execucao_status=r.get("ultima_execucao_status"),
            ultimo_parquet=r.get("ultimo_parquet"),
        )
        for r in rows
    ]
# ============================================================
# CONTROLE INCREMENTAL API
# ============================================================
# ============================================================
# SMB / CAMINHO BRONZE
# ============================================================
def normalizar_nome(valor: str) -> str:
    return re.sub(
        r"[^a-z0-9_]+",
        "_",
        valor.strip().lower(),
    ).strip("_")
def raiz_smb(caminho: str) -> str:
    partes = PureWindowsPath(caminho).parts
    if len(partes) < 2:
        raise ValueError(
            f"Caminho UNC invalido: {caminho}"
        )
    return str(
        PureWindowsPath(
            partes[0],
            partes[1],
        )
    )
def autenticar_smb(cfg: ConfigPombo) -> None:
    if (
        cfg.tipo_destino != "SMB"
        or not cfg.usuario
        or not cfg.senha
    ):
        return
    raiz = raiz_smb(cfg.caminho)
    result = subprocess.run(
        [
            "net",
            "use",
            raiz,
            cfg.senha,
            f"/user:{cfg.usuario}",
            "/persistent:no",
        ],
        capture_output=True,
        text=True,
    )
    texto = (
        result.stdout + result.stderr
    ).strip()
    if (
        result.returncode != 0
        and "1219" not in texto
        and "ja existe" not in texto.lower()
    ):
        raise RuntimeError(
            f"Falha no SMB {raiz}: {texto}"
        )
def pasta_entidade(
    cfg_pombo: ConfigPombo,
    cfg_api: ConfigAPI,
    entidade: Entidade,
) -> Path:
    return (
        Path(cfg_pombo.caminho)
        / "Bronze"
        / normalizar_nome(cfg_api.nome_alias)
        / normalizar_nome(entidade.tabela)
    )
# ============================================================
# PARQUET
# ============================================================
def gravar_parquet(
    registros: list[dict[str, Any]],
    pasta_base: Path,
    prefixo: str,
) -> Path:
    agora = datetime.now()
    pasta = (
        pasta_base
        / f"ingestion_year={agora.year:04d}"
        / f"ingestion_month={agora.month:02d}"
        / f"ingestion_day={agora.day:02d}"
    )
    pasta.mkdir(
        parents=True,
        exist_ok=True,
    )
    arquivo = (
        pasta
        / (
            f"{prefixo}_"
            f"{agora:%Y%m%d_%H%M%S_%f}_"
            f"{uuid.uuid4().hex[:8]}.parquet"
        )
    )
    tabela = pa.Table.from_pylist(registros)
    pq.write_table(
        tabela,
        arquivo,
        compression="zstd",
        use_dictionary=True,
    )
    return arquivo
# ============================================================
# HTTP
# ============================================================
def montar_headers(
    cfg: ConfigAPI,
) -> dict[str, str]:
    headers = {
        str(k): str(v)
        for k, v in cfg.headers_json.items()
    }
    headers.setdefault(
        "Accept",
        "application/json",
    )
    tipo = (
        cfg.tipo_autenticacao
        or ""
    ).upper()
    if (
        tipo == "API_KEY"
        and cfg.api_key
        and cfg.api_key_header
    ):
        headers[cfg.api_key_header] = cfg.api_key
    elif (
        tipo == "BEARER"
        and cfg.token_api
    ):
        headers["Authorization"] = (
            f"Bearer {cfg.token_api}"
        )
    return headers
def executar_request(
    cfg: ConfigAPI,
    params: dict[str, Any],
) -> dict[str, Any]:
    if cfg.metodo_http != "GET":
        raise NotImplementedError(
            "Primeira versão do Pombo API "
            "suporta somente GET."
        )
    url = (
        cfg.endpoint_base
        + cfg.rota_recurso
    )
    headers = montar_headers(cfg)
    ultimo_erro: Exception | None = None
    for tentativa in range(
        1,
        cfg.qtd_tentativas + 1,
    ):
        try:
            resposta = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=cfg.timeout_segundos,
            )
            if not resposta.ok:
                raise RuntimeError(
                    f"HTTP {resposta.status_code} | "
                    f"URL: {resposta.url} | "
                    f"Resposta: {resposta.text}"
                )
            conteudo = resposta.json()
            if not isinstance(
                conteudo,
                dict,
            ):
                raise RuntimeError(
                    "Resposta da API não é "
                    "um objeto JSON."
                )
            return conteudo
        except Exception as exc:
            ultimo_erro = exc
            log.warning(
                "[%s] Tentativa %s/%s falhou: %s",
                cfg.nome_api,
                tentativa,
                cfg.qtd_tentativas,
                exc,
            )
            if tentativa < cfg.qtd_tentativas:
                time.sleep(
                    cfg.intervalo_retry_seg
                )
    raise RuntimeError(
        f"API falhou após "
        f"{cfg.qtd_tentativas} tentativas: "
        f"{ultimo_erro}"
    )
# ============================================================
# CAMPOS CONTASPAGARRECEBER
# ============================================================
# Estrutura de negócio que o extrator API entrega à Bronze.
# Campos sem equivalente confirmado na API ficam NULL.
#
# O objetivo é a API morrer aqui:
# do Bronze para frente, o pipeline trabalha como
# ContasPagarReceber.
CAMPOS_CPR = [
    "CodEmpresa",
    "Tipo",
    "NumLancamento",
    "CodEnt",
    "NumDocto",
    "NumLote",
    "Moeda",
    "ValorOrig",
    "Saldo",
    "Historico",
    "SeqNF",
    "Parcela",
    "CodTpCobr",
    "CodRec",
    "RzSocial",
    "Observ",
    "ValorPago",
    "NumeroNF",
    "NumPedido",
    "CCentidade",
    "CCCustoVenda",
    "CCEstoque",
    "Custo",
    "CCDspCom",
    "CCComAPg",
    "ValComissao",
    "Usuario",
    "COMPRADOR",
    "RZSOCIALCOMPRADOR",
    "sinistro",
    "txJurosDia",
    "NossoNumero",
    "BaixaEletronica",
    "BaixaConfirmada",
    "PostergacaoConfirmada",
    "SeqCC",
    "ProcessadoPelaTabelaPreco",
    "Competencia",
    "convenio",
    "cartorio",
    "Previsao",
    "SeqContabilizacao",
    "PodeContabilizar",
    "SeqLoteReal",
    "SeqPadrao",
    "SeqDocto",
    "Banco",
    "Agencia",
    "Conta",
    "ContadorSeqDocto",
    "ValorBruto",
    "SequencialReneg",
    "Renegociado",
    "InfoAdic",
    "ComandoAuxiliar",
    "ChequeDevolvido",
    "GeraAdiantamento",
    "ContabilizaReal",
    "FatDFin",
    "ValFat",
    "Autorizado",
    "AutorizadoPor",
    "AutorizadoEm",
    "DigAgencia",
    "DigConta",
    "CR_NossoNumero",
    "SeqBanco",
    "CodEmpresaAdto",
    "SeqCreditoAdto",
    "ComAdtoQuantificado",
    "UsuarioCancelamento",
    "SeqContrato",
    "DataInclusao",
    "Carteira",
    "ValorAcrescimoTipoCobr",
    "DataCancelamentoLote",
    "SeqCPRRecorrente",
    "UtilizaConversao",
    "TotalParcelas",
    "SequencialProcessoPMM",
    "SequencialEtapaPMM",
    "ValorContas",
    "CNPJ",
    # nomes crus esperados para as datas da CPR
    "DtEmissao",
    "DtVenctoOrig",
    "DtVencto",
    "DtCancelamento",
    # campos adicionais preservados da API Quality
    "vendaCodigo",
    "duplicataCodigo",
    "tipoOriginal",
    "dataPagamento",
    "planoContaGerencialCodigo",
    "convertido",
    "tituloNumero",
    "dataHoraAtualizacao",
    "codigo",
]
def carregar_depara_entidade(
    hub: psycopg.Connection,
    entidade: Entidade,
) -> list[dict[str, str]]:
    """Carrega o de/para ativo da entidade atual."""
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                nome_tabela,
                campo_origem,
                campo_destino
            FROM public.de_para_entidade
            WHERE id_entidade = %s
              AND ativo = TRUE
            ORDER BY id_de_para
        """, (entidade.id,))
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError(
            f"Nenhum de/para ativo encontrado para a entidade "
            f"{entidade.id} ({entidade.tabela})."
        )
    return [
        {
            "nome_tabela": row["nome_tabela"],
            "campo_origem": row["campo_origem"],
            "campo_destino": row["campo_destino"],
        }
        for row in rows
    ]
def harmonizar_registro(
    item: dict[str, Any],
    fonte: Fonte,
    cfg_api: ConfigAPI,
    entidade: Entidade,
    operacao: str,
    batch_id: str,
    de_para: list[dict[str, str]],
) -> dict[str, Any]:
    """Aplica exclusivamente o de/para parametrizado no banco."""
    registro: dict[str, Any] = {}
    for regra in de_para:
        campo_origem = regra["campo_origem"]
        campo_destino = regra["campo_destino"]
        if campo_origem.startswith("FIXO:"):
            registro[campo_destino] = campo_origem[5:]  # valor fixo
        else:
            registro[campo_destino] = item.get(campo_origem)
    codigo = (
        item.get("codigo")
        if item.get("codigo") is not None
        else item.get("vendaCodigo")
    )
    registro.update({
        "_source_id": fonte.id,
        "_source_name": fonte.nome,
        "nome_alias": cfg_api.nome_alias,
        "_entity_id": entidade.id,
        "_entity": entidade.tabela,
        "_operation": operacao,
        "_event_id": codigo,
        "_event_table": cfg_api.nome_api,
        "_batch_id": batch_id,
        "_ingestion_timestamp": datetime.now(timezone.utc),
    })
    return registro
# ============================================================
# PAGINAÇÃO / EXTRAÇÃO
# ============================================================
BASE_BRONZE_POSTOS = Path(r"D:\Pipeline\Bronze\postos")
def localizar_ultimo_parquet(entidade: Entidade) -> Path:
    """
    Localiza o último parquet da entidade.
    Primeiro tenta o caminho gravado no controle.
    Se não existir, procura recursivamente dentro da pasta Bronze.
    """
    if entidade.ultimo_parquet:
        arquivo = Path(entidade.ultimo_parquet)
        if arquivo.is_file():
            return arquivo
    candidatos_pasta = [
        BASE_BRONZE_POSTOS / entidade.tabela,
        BASE_BRONZE_POSTOS / entidade.tabela.replace("_", ""),
    ]
    for pasta in candidatos_pasta:
        if pasta.exists():
            arquivos = list(pasta.rglob("*.parquet"))
            if arquivos:
                return max(
                    arquivos,
                    key=lambda x: x.stat().st_mtime
                )
    raise RuntimeError(
        f"[{entidade.tabela}] nenhum parquet encontrado."
    )
def descobrir_coluna_data_parquet(arquivo: Path) -> str:
    """
    Descobre automaticamente a coluna de data do parquet.
    Não depende de nome de API.
    """
    prioridades = [
        "dataMovimento",
        "dataHora",
        "dataHoraAtualizacao",
        "DtEmissao",
        "DataInclusao",
    ]
    colunas = pq.read_schema(arquivo).names
    for coluna in prioridades:
        if coluna in colunas:
            return coluna
    raise RuntimeError(
        f"Nenhuma coluna de data encontrada no parquet: {arquivo}"
    )
def data_inicial_pelo_ultimo_parquet(entidade):
    """
    Usa o último parquet da entidade incremental.
    A coluna de data é descoberta pelo schema.
    """
    arquivo = localizar_ultimo_parquet(entidade)
    coluna_data = descobrir_coluna_data_parquet(arquivo)
    df = pd.read_parquet(
        arquivo,
        columns=[coluna_data]
    )
    datas = pd.to_datetime(
        df[coluna_data],
        errors="coerce"
    ).dropna()
    if datas.empty:
        raise RuntimeError(
            f"[{entidade.tabela}] Nenhuma data válida em {arquivo}"
        )
    maior_data = datas.max().date()
    data_inicial = maior_data - timedelta(days=1)
    log.info(
        "[%s] ultimo_parquet=%s | MAX(%s)=%s | dataInicial=%s",
        entidade.tabela,
        arquivo,
        coluna_data,
        maior_data,
        data_inicial,
    )
    return data_inicial.isoformat()
def extrair_paginas(
    hub: psycopg.Connection,
    cfg_api: ConfigAPI,
    fonte: Fonte,
    entidade: Entidade,
    pasta: Path,
    carga_inicial: bool,
    checkpoint: datetime | None,
) -> tuple[int, Path | None]:
    params_config = dict(
        cfg_api.parametros_json
    )
    tamanho = (
        entidade.tamanho_lote
        or cfg_api.tamanho_pagina
        or 1000
    )
    parametro_limite = (
        cfg_api.parametro_limite
        or "limite"
    )
    parametro_cursor = (
        cfg_api.parametro_pagina
        or "ultimoCodigo"
    )
    campo_cursor = (
        cfg_api.campo_proxima_pagina
        or "ultimoCodigo"
    )
    total = 0
    ultimo_parquet_gerado: Path | None = None
    de_para = (
        carregar_depara_entidade(hub, entidade)
        if cfg_api.harmoniza
        else None
    )
    # ========================================================
    # CARGA INICIAL
    # ========================================================
    #
    # A API não aceita bem o intervalo inteiro de 2015 até o
    # ano atual em uma única chamada.
    #
    # Por isso a carga inicial é quebrada ano a ano:
    #
    # 2015-01-01 -> 2015-12-31
    # 2016-01-01 -> 2016-12-31
    # ...
    # ano atual-01-01 -> ano atual-12-31
    #
    # Dentro de cada ano, ultimoCodigo continua sendo usado
    # somente como cursor de paginação.
    # ========================================================
    if carga_inicial:
        if "dataInicial" not in params_config and "dataFinal" not in params_config:
            params_base = dict(params_config)
            params_base[parametro_limite] = tamanho
            params_base.pop(parametro_cursor, None)
            params_base.pop("dataInicial", None)
            params_base.pop("dataFinal", None)
            params_base.pop("dataHoraAtualizacao", None)
            pagina = 0
            cursor_anterior: Any = None
            cursor_atual: Any = None
            while True:
                pagina += 1
                params = dict(params_base)
                if cursor_atual is not None:
                    params[parametro_cursor] = cursor_atual
                log.info(
                    "[%s/%s] Carga inicial | Página %s | cursor=%s",
                    fonte.nome,
                    entidade.tabela,
                    pagina,
                    cursor_atual,
                )
                payload = executar_request(cfg_api, params)
                dados = payload.get(cfg_api.campo_dados)
                if dados is None:
                    raise RuntimeError(
                        f"Campo de dados {cfg_api.campo_dados!r} "
                        "não encontrado na resposta."
                    )
                if not isinstance(dados, list):
                    raise RuntimeError(
                        f"Campo {cfg_api.campo_dados!r} não é uma lista."
                    )
                if not dados:
                    log.info(
                        "[%s/%s] Carga inicial concluída.",
                        fonte.nome,
                        entidade.tabela,
                    )
                    break
                batch_id = "FULL_" + uuid.uuid4().hex
                if cfg_api.harmoniza:
                    registros = [
                        harmonizar_registro(
                            item=item,
                            fonte=fonte,
                            cfg_api=cfg_api,
                            entidade=entidade,
                            operacao="SNAPSHOT",
                            batch_id=batch_id,
                            de_para=de_para or [],
                        )
                        for item in dados
                    ]
                else:
                    registros = [
                        {
                            **item,
                            "_source_id": fonte.id,
                            "_source_name": fonte.nome,
                            "nome_alias": cfg_api.nome_alias,
                            "_entity_id": entidade.id,
                            "_entity": entidade.tabela,
                            "_operation": "SNAPSHOT",
                            "_event_id": (
                                item.get("codigo")
                                if item.get("codigo") is not None
                                else item.get("vendaCodigo")
                            ),
                            "_event_table": cfg_api.nome_api,
                            "_batch_id": batch_id,
                            "_ingestion_timestamp": datetime.now(timezone.utc),
                        }
                        for item in dados
                    ]
                arquivo = gravar_parquet(
                    registros,
                    pasta,
                    f"full_{pagina:06d}",
                )
                ultimo_parquet_gerado = arquivo
                total += len(registros)
                log.info(
                    "[%s/%s] %s linhas -> %s",
                    fonte.nome,
                    entidade.tabela,
                    len(registros),
                    arquivo,
                )
                if not cfg_api.paginacao_ativa:
                    break
                cursor_anterior = cursor_atual
                cursor_atual = payload.get(campo_cursor)
                if cursor_atual is None:
                    break
                if (
                    cursor_anterior is not None
                    and cursor_atual == cursor_anterior
                ):
                    raise RuntimeError(
                        "Paginação interrompida: ultimoCodigo não avançou."
                    )
            return total, ultimo_parquet_gerado
        data_inicial_config = params_config.get(
            "dataInicial"
        )
        if not data_inicial_config:
            raise RuntimeError(
                "parametros_json.dataInicial não foi configurado "
                "para a carga inicial da API."
            )
        try:
            ano_inicial = datetime.strptime(
                str(data_inicial_config),
                "%Y-%m-%d",
            ).year
        except ValueError as exc:
            raise RuntimeError(
                "dataInicial da configuracao_api deve estar no "
                "formato YYYY-MM-DD."
            ) from exc
        ano_atual = datetime.now().year
        if ano_inicial > ano_atual:
            raise RuntimeError(
                f"Ano inicial {ano_inicial} é maior que "
                f"o ano atual {ano_atual}."
            )
        for ano in range(
            ano_inicial,
            ano_atual + 1,
        ):
            params_base = dict(
                params_config
            )
            params_base[
                "dataInicial"
            ] = f"{ano}-01-01"
            params_base[
                "dataFinal"
            ] = f"{ano}-12-31"
            params_base[
                parametro_limite
            ] = tamanho
            params_base.pop(
                parametro_cursor,
                None,
            )
            params_base.pop(
                "dataHoraAtualizacao",
                None,
            )
            pagina = 0
            cursor_anterior: Any = None
            cursor_atual: Any = None
            log.info(
                "[%s/%s] Carga inicial do ano %s",
                fonte.nome,
                entidade.tabela,
                ano,
            )
            while True:
                pagina += 1
                params = dict(
                    params_base
                )
                if cursor_atual is not None:
                    params[
                        parametro_cursor
                    ] = cursor_atual
                log.info(
                    "[%s/%s] Ano %s | Página %s | cursor=%s",
                    fonte.nome,
                    entidade.tabela,
                    ano,
                    pagina,
                    cursor_atual,
                )
                payload = executar_request(
                    cfg_api,
                    params,
                )
                dados = payload.get(
                    cfg_api.campo_dados
                )
                if dados is None:
                    raise RuntimeError(
                        f"Campo de dados "
                        f"{cfg_api.campo_dados!r} "
                        f"não encontrado na resposta."
                    )
                if not isinstance(
                    dados,
                    list,
                ):
                    raise RuntimeError(
                        f"Campo "
                        f"{cfg_api.campo_dados!r} "
                        f"não é uma lista."
                    )
                if not dados:
                    log.info(
                        "[%s/%s] Ano %s concluído.",
                        fonte.nome,
                        entidade.tabela,
                        ano,
                    )
                    break
                batch_id = (
                    f"FULL_{ano}_"
                    + uuid.uuid4().hex
                )
                if cfg_api.harmoniza:
                    registros = [
                        harmonizar_registro(
                            item=item, fonte=fonte, cfg_api=cfg_api,
                            entidade=entidade, operacao="SNAPSHOT",
                            batch_id=batch_id,
                            de_para=de_para or [],
                        )
                        for item in dados
                    ]
                else:
                    registros = [
                        {
                            **item,
                            "_source_id": fonte.id,
                            "_source_name": fonte.nome,
                            "nome_alias": cfg_api.nome_alias,
                            "_entity_id": entidade.id,
                            "_entity": entidade.tabela,
                            "_operation": "SNAPSHOT",
                            "_event_id": (item.get("codigo") if item.get("codigo") is not None else item.get("vendaCodigo")),
                            "_event_table": cfg_api.nome_api,
                            "_batch_id": batch_id,
                            "_ingestion_timestamp": datetime.now(timezone.utc),
                        }
                        for item in dados
                    ]
                arquivo = gravar_parquet(
                    registros,
                    pasta,
                    (
                        f"full_{ano}_"
                        f"{pagina:06d}"
                    ),
                )
                ultimo_parquet_gerado = arquivo
                total += len(
                    registros
                )
                log.info(
                    "[%s/%s] Ano %s | %s linhas -> %s",
                    fonte.nome,
                    entidade.tabela,
                    ano,
                    len(registros),
                    arquivo,
                )
                if not cfg_api.paginacao_ativa:
                    break
                cursor_anterior = (
                    cursor_atual
                )
                cursor_atual = (
                    payload.get(
                        campo_cursor
                    )
                )
                if cursor_atual is None:
                    log.info(
                        "[%s/%s] Ano %s | "
                        "API não retornou novo cursor. Fim.",
                        fonte.nome,
                        entidade.tabela,
                        ano,
                    )
                    break
                if (
                    cursor_anterior is not None
                    and cursor_atual == cursor_anterior
                ):
                    raise RuntimeError(
                        f"Paginação interrompida no ano {ano}: "
                        "ultimoCodigo não avançou."
                    )
        return total, ultimo_parquet_gerado
    # ========================================================
    # INCREMENTAL
    # ========================================================
    if checkpoint is None:
        raise RuntimeError(
            "Checkpoint não informado para carga incremental."
        )
    params_base = dict(
        params_config
    )
    params_base[
        parametro_limite
    ] = tamanho
    params_base.pop(
        parametro_cursor,
        None,
    )
    inicio_incremental = (
        checkpoint
        - timedelta(minutes=1)
    )
    # A API Quality declara dataHoraAtualizacao como OffsetDateTime.
    # O checkpoint vindo do PostgreSQL pode chegar sem tzinfo.
    # Nesse caso, preservamos o horário gravado e adicionamos
    # explicitamente o offset de Brasília (-03:00).
    if inicio_incremental.tzinfo is None:
        inicio_incremental = inicio_incremental.replace(
            tzinfo=timezone(
                timedelta(hours=-3)
            )
        )
    # Filtro temporal definido pelo parametros_json da configuracao_api:
    # se tem dataInicial/dataFinal no cadastro, a API suporta filtro por data.
    suporta_filtro_data = (
        cfg_api.gravadata
        and ("dataInicial" in params_config or "dataFinal" in params_config)
    )
    if not suporta_filtro_data:
        params_base.pop("dataInicial", None)
        params_base.pop("dataFinal", None)
        params_base.pop("dataHoraAtualizacao", None)
    elif entidade.tipo_carga.upper() == "INCREMENTAL":
        params_base["dataInicial"] = data_inicial_pelo_ultimo_parquet(entidade)
        params_base["dataFinal"] = datetime.now().astimezone().date().isoformat()
        params_base.pop("dataHoraAtualizacao", None)
    else:
        params_base["dataHoraAtualizacao"] = inicio_incremental.isoformat()
    pagina = 0
    cursor_anterior: Any = None
    cursor_atual: Any = None
    while True:
        pagina += 1
        params = dict(
            params_base
        )
        if cursor_atual is not None:
            params[
                parametro_cursor
            ] = cursor_atual
        log.info(
            "[%s/%s] Incremental | Página %s | cursor=%s",
            fonte.nome,
            entidade.tabela,
            pagina,
            cursor_atual,
        )
        payload = executar_request(
            cfg_api,
            params,
        )
        dados = payload.get(
            cfg_api.campo_dados
        )
        if dados is None:
            raise RuntimeError(
                f"Campo de dados "
                f"{cfg_api.campo_dados!r} "
                f"não encontrado na resposta."
            )
        if not isinstance(
            dados,
            list,
        ):
            raise RuntimeError(
                f"Campo "
                f"{cfg_api.campo_dados!r} "
                f"não é uma lista."
            )
        if not dados:
            log.info(
                "[%s/%s] Incremental concluído.",
                fonte.nome,
                entidade.tabela,
            )
            break
        batch_id = (
            "INC_"
            + uuid.uuid4().hex
        )
        if cfg_api.harmoniza:
            registros = [
                harmonizar_registro(
                    item=item, fonte=fonte, cfg_api=cfg_api,
                    entidade=entidade, operacao="UPDATE",
                    batch_id=batch_id,
                    de_para=de_para or [],
                )
                for item in dados
            ]
        else:
            registros = [
                {
                    **item,
                    "_source_id": fonte.id,
                    "_source_name": fonte.nome,
                    "nome_alias": cfg_api.nome_alias,
                    "_entity_id": entidade.id,
                    "_entity": entidade.tabela,
                    "_operation": "UPDATE",
                    "_event_id": (item.get("codigo") if item.get("codigo") is not None else item.get("vendaCodigo")),
                    "_event_table": cfg_api.nome_api,
                    "_batch_id": batch_id,
                    "_ingestion_timestamp": datetime.now(timezone.utc),
                }
                for item in dados
            ]
        arquivo = gravar_parquet(
            registros,
            pasta,
            f"api_{pagina:06d}",
        )
        ultimo_parquet_gerado = arquivo
        total += len(
            registros
        )
        log.info(
            "[%s/%s] %s linhas -> %s",
            fonte.nome,
            entidade.tabela,
            len(registros),
            arquivo,
        )
        if not cfg_api.paginacao_ativa:
            break
        cursor_anterior = (
            cursor_atual
        )
        cursor_atual = (
            payload.get(
                campo_cursor
            )
        )
        if cursor_atual is None:
            log.info(
                "[%s/%s] API não retornou novo cursor. Fim.",
                fonte.nome,
                entidade.tabela,
            )
            break
        if (
            cursor_anterior is not None
            and cursor_atual == cursor_anterior
        ):
            raise RuntimeError(
                "Paginação incremental interrompida: "
                "ultimoCodigo não avançou."
            )
    return total, ultimo_parquet_gerado
# ============================================================
# FINALIZAÇÃO / CONTROLES
# ============================================================
def concluir_carga_inicial(
    hub: psycopg.Connection,
    cfg_api: ConfigAPI,
    entidade: Entidade,
    checkpoint_execucao: datetime,
    ultimo_parquet: Path | None,
) -> None:
    """
    Primeira carga concluída com sucesso.
    Usa somente estruturas já existentes:
    - configuracao_entidades
    - configuracao_api
    Após concluir:
    1. marca carga_inicial_concluida = TRUE;
    2. grava ultima_execucao com a fronteira capturada antes da extração;
    3. troca dataInicial da API para 2026-01-01.
    """
    with hub.cursor() as cur:
        cur.execute("""
            UPDATE public.configuracao_entidades
            SET
                carga_inicial_concluida = TRUE,
                data_carga_inicial = CURRENT_TIMESTAMP,
                ultima_execucao = %s,
                proxima_execucao =
                    %s + (%s * INTERVAL '1 minute'),
                ultima_execucao_status = 'SUCESSO',
                ultimo_parquet = COALESCE(%s, ultimo_parquet)
            WHERE id = %s
        """, (
            checkpoint_execucao,
            checkpoint_execucao,
            entidade.intervalo_minutos,
            str(ultimo_parquet) if ultimo_parquet else None,
            entidade.id,
        ))
        if cfg_api.gravadata:
            ano_atual = datetime.now().year
            data_inicial_atual = f"{ano_atual}-01-01"
            data_final_atual = f"{ano_atual}-12-31"
            cur.execute("""
                UPDATE public.configuracao_api
                SET
                    parametros_json =
                        jsonb_set(
                            jsonb_set(
                                COALESCE(
                                    parametros_json,
                                    '{}'::jsonb
                                ),
                                '{dataInicial}',
                                to_jsonb(%s::text),
                                TRUE
                            ),
                            '{dataFinal}',
                            to_jsonb(%s::text),
                            TRUE
                        ),
                    data_alteracao = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data_inicial_atual,
                data_final_atual,
                cfg_api.id,
            ))
    hub.commit()
def concluir_incremental(
    hub: psycopg.Connection,
    entidade: Entidade,
    checkpoint_execucao: datetime,
    ultimo_parquet: Path | None,
) -> None:
    """
    O próprio configuracao_entidades.ultima_execucao
    é o checkpoint confirmado da API.
    Só avança depois que todas as páginas foram
    extraídas e gravadas com sucesso.
    """
    with hub.cursor() as cur:
        cur.execute("""
            UPDATE public.configuracao_entidades
            SET
                ultima_execucao = %s,
                proxima_execucao =
                    %s + (%s * INTERVAL '1 minute'),
                ultima_execucao_status = 'SUCESSO',
                ultimo_parquet = COALESCE(%s, ultimo_parquet)
            WHERE id = %s
        """, (
            checkpoint_execucao,
            checkpoint_execucao,
            entidade.intervalo_minutos,
            str(ultimo_parquet) if ultimo_parquet else None,
            entidade.id,
        ))
    hub.commit()
def marcar_entidade_erro(
    hub: psycopg.Connection,
    entidade: Entidade,
) -> None:
    """
    Em caso de erro, altera apenas o status.
    ultima_execucao não é avançada para não perder
    o ponto do incremental.
    """
    with hub.cursor() as cur:
        cur.execute("""
            UPDATE public.configuracao_entidades
            SET ultima_execucao_status = 'ERRO'
            WHERE id = %s
        """, (
            entidade.id,
        ))
    hub.commit()
# ============================================================
# PROCESSAMENTO
# ============================================================
def processar_entidade(
    hub: psycopg.Connection,
    fonte: Fonte,
    cfg_api: ConfigAPI,
    entidade: Entidade,
    cfg_pombo: ConfigPombo,
) -> None:
    pasta = pasta_entidade(
        cfg_pombo,
        cfg_api,
        entidade,
    )
    # A fronteira é capturada ANTES de iniciar a API.
    # Só vira checkpoint depois de toda a carga concluir.
    checkpoint_execucao = (
        datetime.now().astimezone()
    )
    carga_inicial = (
        not entidade.carga_inicial_concluida
    )
    checkpoint_anterior = (
        None
        if carga_inicial
        else (
            entidade.ultima_execucao
            or entidade.data_carga_inicial
        )
    )
    if (
        not carga_inicial
        and checkpoint_anterior is None
    ):
        raise RuntimeError(
            "Entidade está com carga inicial concluída, "
            "mas ultima_execucao/data_carga_inicial estão vazias."
        )
    log.info(
        "[%s/%s] Início | modo=%s | checkpoint=%s",
        fonte.nome,
        entidade.tabela,
        (
            "CARGA_INICIAL"
            if carga_inicial
            else "INCREMENTAL"
        ),
        checkpoint_anterior,
    )
    total, ultimo_parquet_gerado = extrair_paginas(
        hub=hub,
        cfg_api=cfg_api,
        fonte=fonte,
        entidade=entidade,
        pasta=pasta,
        carga_inicial=carga_inicial,
        checkpoint=checkpoint_anterior,
    )
    if carga_inicial:
        concluir_carga_inicial(
            hub=hub,
            cfg_api=cfg_api,
            entidade=entidade,
            checkpoint_execucao=checkpoint_execucao,
            ultimo_parquet=ultimo_parquet_gerado,
        )
        log.info(
            "[%s/%s] Carga inicial concluída: "
            "%s linhas. configuracao_api ajustada "
            "para o ano atual.",
            fonte.nome,
            entidade.tabela,
            total,
        )
    else:
        concluir_incremental(
            hub=hub,
            entidade=entidade,
            checkpoint_execucao=checkpoint_execucao,
            ultimo_parquet=ultimo_parquet_gerado,
        )
        log.info(
            "[%s/%s] Incremental concluído: %s linhas.",
            fonte.nome,
            entidade.tabela,
            total,
        )
# ============================================================
# CICLO
# ============================================================
def ciclo() -> None:
    with hub_connect() as hub:
        cfg_pombo = buscar_config_pombo(
            hub
        )
        autenticar_smb(
            cfg_pombo
        )
        fontes = buscar_fontes_api(
            hub
        )
        for fonte in fontes:
            try:
                cfg_api = buscar_config_api(
                    hub,
                    fonte.id_referencia,
                )
                entidades = buscar_entidades(
                    hub,
                    fonte.id,
                )
                if not entidades:
                    continue
                for entidade in entidades:
                    try:
                        processar_entidade(
                            hub=hub,
                            fonte=fonte,
                            cfg_api=cfg_api,
                            entidade=entidade,
                            cfg_pombo=cfg_pombo,
                        )
                    except Exception:
                        marcar_entidade_erro(
                            hub,
                            entidade,
                        )
                        log.exception(
                            "[%s/%s] Falha",
                            fonte.nome,
                            entidade.tabela,
                        )
            except Exception:
                log.exception(
                    "[%s] Falha na fonte API",
                    fonte.nome,
                )
# ============================================================
# MAIN
# ============================================================
def main() -> int:
    log.info(
        "Pombo Extrator API iniciado"
    )
    try:
        ciclo()
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        log.exception(
            "Erro geral no ciclo API"
        )
        return 1
def executar() -> int:
    return main()
if __name__ == "__main__":
    raise SystemExit(
        executar()
    )