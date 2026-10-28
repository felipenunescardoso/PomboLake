from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
import psycopg
from psycopg import sql
from psycopg.rows import dict_row


# ============================================================
# CONFIGURAÇÕES
# ============================================================
# Mapeamento dinâmico:
#   configuracao_entidades.nome_tabela = Sybase/Bronze
#   configuracao_entidades.tabela_hub  = Gold/Hub
#   parquet = tabela_hub + '.parquet'
#   destino = gold.tabela_hub

CONFIGURACAO_POMBO_ID = 1
SUBPASTA_GOLD = "Gold"
SCHEMA_DESTINO = "gold"
TAMANHO_LOTE = 10_000

COLUNA_OPERACAO = "_operation"

COLUNAS_CONTROLE = {
    "_source_id",
    "_source_name",
    "_entity_id",
    "_entity",
    "_event_table",
    "_operation",
    "_event_id",
    "_batch_id",
    "_ingestion_timestamp",
}

METADADOS_OBRIGATORIOS = {
    "_source_id",
    "_entity_id",
    "_entity",
    "_event_table",
    "_operation",
    "_event_id",
    "_batch_id",
}

OPERACOES_DELETE = ("DELETE", "DELETED", "D")

ALIASES_API_LEGADOS = {"VENDAS_POSTOS"}

def normalizar_alias(nome_alias: Any) -> str:
    return str(nome_alias or "").strip().upper()

def eh_alias_api(nome_alias: Any) -> bool:
    alias = normalizar_alias(nome_alias)
    return alias == "POSTOS" or alias.startswith("POSTOS_") or alias in ALIASES_API_LEGADOS


CHAVES_UPSERT: dict[str, list[str]] = {
    "contas_pagar_receber": ["cpr_key"],
    "contas_pagar_receber_moeda": ["cpr_key"],
    "nota_fiscal": ["nota_fiscal_key"],
    "itens_nf": ["item_nota_fiscal_key"],
    "movimento_plano_contas": [
        "nome_alias", "cod_empresa", "num_lote", "cod_conta"
    ],
    "centro_custo_rateio": [
        "nome_alias", "cod_empresa", "num_lote", "seq_cc"
    ],
    "centro_custo_pagar": [
        "nome_alias", "cod_empresa", "num_lancamento", "seq_cc"
    ],
    "z_nota_fiscal_classificacao_despesas": [
        "nome_alias",
        "cod_empresa",
        "sequencial_nf",
        "id_classificacao_despesas",
    ],
    "empresas": ["empresa_key"],
    "entidade": ["entidade_key"],
    "vendedor": ["vendedor_key"],
    "portador": ["portador_key"],
    "tipo_documento": ["tipo_documento_key"],
    "tipo_cobranca": ["cod_tp_cobr_key"],
    "natureza_operacao": ["natureza_operacao_key"],
    "z_classificacao_despesas": ["classificacao_despesa_key"],
    "plano_contas": ["plano_conta_key"],
    "centro_custo": ["centro_custo_key"],
    "pedido": ["pedido_key"],
    "itens_pedido": ["nome_alias", "cod_empresa", "num_ped", "seq"],
    "cidade": ["nome_alias", "codigo_dnc"],
    "bandeira": ["nome_alias", "cod_bandeira"],
    "baixa_antecipada": ["baixa_antecipada_key"],
    "itens_baixa": ["nome_alias", "cod_empresa", "num_baixa", "item"],
    "baixa": ["baixa_key"],
    "produto": ["nome_alias", "cod_prod"],
    "deposito": ["nome_alias", "cod_dep"],
    "vendas_postos": ["venda_key"],
    "vendas_postos_forma_pagamento": ["venda_forma_pagamento_key"],
    "vendas_postos_itens": ["venda_item_key"],
    "postos_abastecimentos": ["empresa_codigo", "abastecimento_codigo"],
    "postos_duplicatas": ["empresa_codigo", "duplicata_codigo"],
    "postos_movimentos_conta": ["empresa_codigo", "movimento_conta_codigo"],
    "postos_tanques": ["nome_alias", "empresa_codigo", "tanque_codigo"],
    "postos_tanques_entradas": [
        "nome_alias", "tanque_codigo", "tanque_entrada_codigo"
    ],
    "postos_tanques_medidas": [
        "nome_alias", "tanque_codigo", "tanque_medida_codigo"
    ],
    "postos_caixas": ["nome_alias", "empresa_codigo", "caixa_codigo"],
    "postos_sangrias_caixa": [
        "nome_alias", "empresa_codigo", "sangria_codigo"
    ],
    "postos_cartoes": ["nome_alias", "empresa_codigo", "cartao_codigo"],
}


@dataclass(frozen=True)
class ConfigPostgres:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class ConfigEntidade:
    id_entidade: int
    id_fonte: int
    nome_tabela: str
    tabela_hub: str
    tipo_carga: str
    usar_trigger: bool
    carga_inicial_concluida: bool


@dataclass
class ControleSincronizacao:
    cargas_concluidas: set[int] = field(default_factory=set)
    eventos_confirmados: dict[tuple[int, str], int] = field(
        default_factory=dict
    )
    eventos_sem_historico: set[tuple[int, str]] = field(default_factory=set)
    api_confirmados: dict[tuple[int, str], Any] = field(
        default_factory=dict
    )


@dataclass
class ConfirmacaoEntidade:
    id_entidade: int
    id_fonte: int
    nome_alias: str
    nome_entidade: str
    primeira_carga: bool
    eventos: dict[str, tuple[int, str | None]] = field(
        default_factory=dict
    )
    ultimo_ingestion_ts: Any | None = None


@dataclass(frozen=True)
class ResultadoCarga:
    enviados: int
    upserts: int
    excluidos: int
    ignorados: int
    confirmacoes: tuple[ConfirmacaoEntidade, ...]
    checkpoints_api_bootstrap: tuple[tuple[int, str, Any], ...] = ()
    checkpoints_sybase_bootstrap: tuple[tuple[int, int, str, str, str, int, str | None], ...] = ()
    estados_sybase_sem_eventos: tuple[tuple[int, int, str, str, str], ...] = ()


def configurar_logging() -> None:
    pasta_log = Path(__file__).resolve().parent / "logs"
    pasta_log.mkdir(parents=True, exist_ok=True)

    arquivo_log = pasta_log / "orquestrador_dw.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(arquivo_log, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def carregar_configuracoes() -> tuple[ConfigPostgres, ConfigPostgres]:
    host = os.getenv("HUB_CONTROLE_HOST", "").strip()
    porta_texto = os.getenv("HUB_CONTROLE_PORT", "54320").strip()

    valores = {
        "HUB_CONTROLE_HOST": host,
        "HUB_CONTROLE_USER": os.getenv(
            "HUB_CONTROLE_USER", ""
        ).strip(),
        "HUB_CONTROLE_PASSWORD": os.getenv(
            "HUB_CONTROLE_PASSWORD", ""
        ),
        "HUB_DW_USER": os.getenv("HUB_DW_USER", "").strip(),
        "HUB_DW_PASSWORD": os.getenv("HUB_DW_PASSWORD", ""),
    }

    faltantes = [nome for nome, valor in valores.items() if not valor]

    if faltantes:
        raise RuntimeError(
            "Variaveis de ambiente ausentes: " + ", ".join(faltantes)
        )

    try:
        porta = int(porta_texto)
    except ValueError as exc:
        raise RuntimeError(
            "HUB_CONTROLE_PORT deve ser um numero inteiro."
        ) from exc

    central = ConfigPostgres(
        host=host,
        port=porta,
        dbname=os.getenv("HUB_CONTROLE_DB", "central"),
        user=valores["HUB_CONTROLE_USER"],
        password=valores["HUB_CONTROLE_PASSWORD"],
    )

    hub_dw = ConfigPostgres(
        host=host,
        port=porta,
        dbname=os.getenv("HUB_DW_DB", "hub_dw"),
        user=valores["HUB_DW_USER"],
        password=valores["HUB_DW_PASSWORD"],
    )

    return central, hub_dw


def conectar_postgres(
    config: ConfigPostgres,
    application_name: str,
) -> psycopg.Connection[Any]:
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        connect_timeout=15,
        application_name=application_name,
        row_factory=dict_row,
    )


def carregar_controles(
    central: psycopg.Connection[Any],
) -> ControleSincronizacao:
    controle = ControleSincronizacao()

    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT id_entidade
            FROM public.controle_carga_dw
            WHERE carga_inicial_concluida IS TRUE;
            """
        )

        controle.cargas_concluidas = {
            int(linha["id_entidade"])
            for linha in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT
                id_entidade,
                tabela_evento,
                ultimo_event_id_confirmado
            FROM public.controle_evento_dw
            WHERE ultimo_event_id_confirmado IS NOT NULL;
            """
        )

        controle.eventos_confirmados = {
            (
                int(linha["id_entidade"]),
                str(linha["tabela_evento"]),
            ): int(linha["ultimo_event_id_confirmado"])
            for linha in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT id_entidade, tabela_evento
            FROM public.controle_evento_dw
            WHERE ultimo_event_id_confirmado IS NULL
              AND status = 'SEM_EVENTOS';
            """
        )
        controle.eventos_sem_historico = {
            (int(linha["id_entidade"]), str(linha["tabela_evento"]))
            for linha in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT id_entidade, nome_alias, ultimo_ingestion_ts
            FROM public.controle_api_dw
            WHERE ultimo_ingestion_ts IS NOT NULL
              AND status = 'SUCESSO';
            """
        )
        controle.api_confirmados = {
            (
                int(linha["id_entidade"]),
                str(linha["nome_alias"]).strip().upper(),
            ): linha["ultimo_ingestion_ts"]
            for linha in cursor.fetchall()
        }

    return controle


def carregar_configuracoes_entidades(
    central: psycopg.Connection[Any],
) -> dict[int, ConfigEntidade]:
    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                id_fonte,
                nome_tabela,
                tabela_hub,
                tipo_carga,
                usar_trigger,
                carga_inicial_concluida
            FROM public.configuracao_entidades
            WHERE ativo IS TRUE
            ORDER BY id;
            """
        )
        linhas = cursor.fetchall()

    configuracoes: dict[int, ConfigEntidade] = {}

    for linha in linhas:
        id_entidade = int(linha["id"])
        tabela_hub = str(linha["tabela_hub"] or "").strip()

        if not tabela_hub:
            logging.warning(
                "Entidade id=%s / %s sem tabela_hub. Ignorada.",
                id_entidade,
                linha["nome_tabela"],
            )
            continue

        configuracoes[id_entidade] = ConfigEntidade(
            id_entidade=id_entidade,
            id_fonte=int(linha["id_fonte"]),
            nome_tabela=str(linha["nome_tabela"]),
            tabela_hub=tabela_hub,
            tipo_carga=str(linha["tipo_carga"] or "").strip().upper(),
            usar_trigger=bool(linha["usar_trigger"]),
            carga_inicial_concluida=bool(
                linha["carga_inicial_concluida"]
            ),
        )

    if not configuracoes:
        raise RuntimeError(
            "Nenhuma entidade ativa com tabela_hub preenchida."
        )

    return configuracoes


def listar_tabelas_hub(
    configuracoes: dict[int, ConfigEntidade],
) -> list[str]:
    return list(
        dict.fromkeys(
            config.tabela_hub
            for config in configuracoes.values()
        )
    )


def listar_tabelas_hub_api_primeiro(
    central: psycopg.Connection[Any],
    configuracoes: dict[int, ConfigEntidade],
) -> list[str]:
    # A prioridade vem do relacionamento real:
    # configuracao_api -> fontes.id_referencia -> configuracao_entidades.id_fonte.
    # Reutiliza somente ordenacoes ja existentes no Central.
    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT ce.tabela_hub
            FROM public.configuracao_api AS ca
            INNER JOIN public.fontes AS f
                ON UPPER(f.tipo_fonte) = 'API'
               AND f.id_referencia = ca.id
            INNER JOIN public.configuracao_entidades AS ce
                ON ce.id_fonte = f.id
            WHERE ca.ativo IS TRUE
              AND f.ativo IS TRUE
              AND ce.ativo IS TRUE
              AND ce.tabela_hub IS NOT NULL
              AND TRIM(ce.tabela_hub) <> ''
            ORDER BY ca.id DESC, ce.prioridade, ce.id;
            """
        )
        tabelas_api = [
            str(linha["tabela_hub"]).strip()
            for linha in cursor.fetchall()
        ]

    tabelas_normais = listar_tabelas_hub(configuracoes)

    # dict.fromkeys preserva a ordem e impede executar duas vezes uma
    # tabela compartilhada entre API e Sybase.
    return list(dict.fromkeys(tabelas_api + tabelas_normais))


def obter_pasta_gold(
    central: psycopg.Connection[Any],
) -> Path:
    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT caminho
            FROM public.configuracao_pombo
            WHERE id = %s
              AND ativo IS TRUE
            LIMIT 1;
            """,
            (CONFIGURACAO_POMBO_ID,),
        )
        linha = cursor.fetchone()

    if linha is None:
        raise RuntimeError(
            "Configuracao ativa nao encontrada em "
            f"public.configuracao_pombo para id={CONFIGURACAO_POMBO_ID}."
        )

    caminho = str(linha["caminho"] or "").strip()
    if not caminho:
        raise RuntimeError(
            "public.configuracao_pombo.caminho esta vazio para "
            f"id={CONFIGURACAO_POMBO_ID}."
        )

    pasta_base = Path(caminho)
    if pasta_base.name.lower() == SUBPASTA_GOLD.lower():
        return pasta_base

    return pasta_base / SUBPASTA_GOLD


def listar_colunas_postgres(
    conexao: psycopg.Connection[Any],
    schema: str,
    tabela: str,
) -> list[str]:
    consulta = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position;
    """

    with conexao.cursor() as cursor:
        cursor.execute(consulta, (schema, tabela))
        return [str(linha["column_name"]) for linha in cursor.fetchall()]


def normalizar_valor(valor: Any) -> Any:
    if isinstance(valor, pa.Scalar):
        valor = valor.as_py()

    if valor is None:
        return None

    if isinstance(valor, bytes):
        try:
            valor = valor.decode("utf-8", errors="replace")
        except Exception:
            valor = valor.decode("latin1", errors="replace")

    if isinstance(valor, str):
        valor = valor.replace("\x00", "")
        valor = "".join(
            caractere
            for caractere in valor
            if caractere in ("\t", "\n", "\r")
            or ord(caractere) >= 32
        )
        valor = valor.strip()

    return valor


def validar_colunas(
    arquivo_parquet: Path,
    colunas_parquet: list[str],
    colunas_postgres: list[str],
    tabela: str,
) -> None:
    conjunto_parquet = set(colunas_parquet)
    conjunto_postgres = set(colunas_postgres)

    faltando_no_parquet = [
        coluna
        for coluna in colunas_postgres
        if coluna not in conjunto_parquet
    ]

    metadados_ausentes = sorted(
        METADADOS_OBRIGATORIOS - conjunto_parquet
    )

    extras_nao_permitidas = [
        coluna
        for coluna in colunas_parquet
        if coluna not in conjunto_postgres
        and coluna not in COLUNAS_CONTROLE
    ]

    if (
        faltando_no_parquet
        or metadados_ausentes
        or extras_nao_permitidas
    ):
        partes = [
            f"Incompatibilidade entre {arquivo_parquet.name} e "
            f"{SCHEMA_DESTINO}.{tabela}."
        ]

        if faltando_no_parquet:
            partes.append(
                "Colunas obrigatorias ausentes no Parquet: "
                + ", ".join(faltando_no_parquet)
            )

        if metadados_ausentes:
            partes.append(
                "Metadados de sincronizacao ausentes: "
                + ", ".join(metadados_ausentes)
            )

        if extras_nao_permitidas:
            partes.append(
                "Colunas extras nao permitidas no Parquet: "
                + ", ".join(extras_nao_permitidas)
            )

        raise RuntimeError(" ".join(partes))


def garantir_indice_unico(
    conexao: psycopg.Connection[Any],
    schema: str,
    tabela: str,
    chaves: list[str],
) -> None:
    nome_indice = f"ux_{tabela}_pombo"

    comando = sql.SQL(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})"
    ).format(
        sql.Identifier(nome_indice),
        sql.Identifier(schema),
        sql.Identifier(tabela),
        sql.SQL(", ").join(
            sql.Identifier(coluna)
            for coluna in chaves
        ),
    )

    with conexao.cursor() as cursor:
        cursor.execute(comando)


def montar_condicao_delete(
    chaves: list[str],
) -> sql.Composed:
    return sql.SQL(" AND ").join(
        sql.SQL(
            "destino.{} IS NOT DISTINCT FROM origem.{}"
        ).format(
            sql.Identifier(chave),
            sql.Identifier(chave),
        )
        for chave in chaves
    )


def _qid(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def obter_identidades_hub_central(
    central: psycopg.Connection[Any],
    tabela: str,
) -> list[dict[str, Any]]:
    inicio = time.perf_counter()

    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                ce.id AS id_entidade,
                ce.id_fonte,
                f.id AS fonte_id_encontrada,
                f.tipo_fonte,
                f.nome_fonte AS nome_fonte,
                ca.nome_alias AS nome_alias_api,
                ce.tipo_carga,
                ce.carga_inicial_concluida
            FROM public.configuracao_entidades AS ce
            LEFT JOIN public.fontes AS f
                ON f.id = ce.id_fonte
            LEFT JOIN public.configuracao_api AS ca
                ON UPPER(f.tipo_fonte) = 'API'
               AND ca.id = f.id_referencia
               AND ca.ativo IS TRUE
            WHERE ce.ativo IS TRUE
              AND ce.tabela_hub = %s
            ORDER BY ce.id;
            """,
            (tabela,),
        )
        identidades = [dict(linha) for linha in cursor.fetchall()]

    for item in identidades:
        id_entidade = item.get("id_entidade")
        id_fonte = item.get("id_fonte")
        fonte_id_encontrada = item.get("fonte_id_encontrada")
        tipo_fonte = normalizar_alias(item.get("tipo_fonte"))

        if fonte_id_encontrada is None:
            mensagem = (
                "[CENTRAL][ERRO] Entidade ativa sem fonte correspondente | "
                f"id_entidade={id_entidade} | id_fonte={id_fonte} | tabela_hub={tabela}"
            )
            logging.error(mensagem)
            raise RuntimeError(mensagem)

        if tipo_fonte == "API":
            nome_alias = normalizar_alias(item.get("nome_alias_api"))
            if not nome_alias:
                mensagem = (
                    "[CENTRAL][ERRO] Entidade API ativa sem nome_alias fisico "
                    "em configuracao_api | "
                    f"id_entidade={id_entidade} | id_fonte={id_fonte} | tabela_hub={tabela}"
                )
                logging.error(mensagem)
                raise RuntimeError(mensagem)
        else:
            nome_alias = normalizar_alias(item.get("nome_fonte"))
            if not nome_alias:
                mensagem = (
                    "[CENTRAL][ERRO] Entidade ativa com nome_fonte nulo/vazio | "
                    f"id_entidade={id_entidade} | id_fonte={id_fonte} | tabela_hub={tabela}"
                )
                logging.error(mensagem)
                raise RuntimeError(mensagem)

        # API usa o alias fisico cadastrado em configuracao_api.
        # Fontes normais/Sybase preservam exatamente fontes.nome_fonte.
        item["nome_alias"] = nome_alias

    logging.info(
        "[PERF] Identidades Central | %s | %.4f segundos | %s identidade(s).",
        tabela,
        time.perf_counter() - inicio,
        len(identidades),
    )
    return identidades


def separar_identidades_hub(
    identidades: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    identidades_sybase: list[dict[str, Any]] = []
    identidades_api: list[dict[str, Any]] = []

    for item in identidades:
        nome_alias = normalizar_alias(item.get("nome_alias"))
        item_normalizado = dict(item)
        item_normalizado["nome_alias"] = nome_alias

        tipo_fonte = normalizar_alias(item.get("tipo_fonte"))

        if tipo_fonte == "API" or eh_alias_api(nome_alias):
            identidades_api.append(item_normalizado)
        else:
            identidades_sybase.append(item_normalizado)

    return identidades_sybase, identidades_api

def obter_bootstrap_api(
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
    id_entidade: int,
    nome_alias: str,
) -> tuple[int, Any | None]:
    # Executado SOMENTE quando ainda nao existe checkpoint em controle_api_dw.
    inicio = time.perf_counter()
    linha = duck.execute(
        """
        SELECT
            COUNT(*) AS qtd_gold,
            MAX(CAST(_ingestion_timestamp AS TIMESTAMPTZ)) AS max_ingestion_ts
        FROM read_parquet(?)
        WHERE CAST(_entity_id AS BIGINT) = ?
          AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?
        """,
        [str(arquivo_parquet), id_entidade, nome_alias],
    ).fetchone()
    logging.info(
        "[PERF] API BOOTSTRAP scan | %s | entidade=%s | alias=%s | %.4f segundos.",
        arquivo_parquet.name,
        id_entidade,
        nome_alias,
        time.perf_counter() - inicio,
    )
    return int(linha[0] or 0), linha[1]


def construir_filtro_api(
    identidades_api: list[dict[str, Any]],
    controle: ControleSincronizacao,
    configuracoes: dict[int, ConfigEntidade],
    hub_dw: psycopg.Connection[Any],
    central: psycopg.Connection[Any],
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
    tabela: str,
) -> tuple[list[str], list[Any], list[tuple[int, str, Any]]]:

    condicoes: list[str] = []
    parametros: list[Any] = []
    checkpoints_bootstrap: list[tuple[int, str, Any]] = []

    for item in identidades_api:
        id_entidade_bruto = item.get("id_entidade")

        if id_entidade_bruto is None:
            continue

        id_entidade = int(id_entidade_bruto)

        if id_entidade not in configuracoes:
            continue

        nome_alias = normalizar_alias(item.get("nome_alias"))

        if not eh_alias_api(nome_alias):
            continue

        chave_api = (id_entidade, nome_alias)
        ultimo_api = controle.api_confirmados.get(chave_api)

        # ============================================================
        # CAMINHO NORMAL / INCREMENTAL
        # ============================================================
        if ultimo_api is not None:
            condicoes.append(
                "(CAST(_entity_id AS BIGINT) = ? "
                "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ? "
                "AND CAST(_ingestion_timestamp AS TIMESTAMPTZ) >= "
                "CAST(? AS TIMESTAMPTZ) - INTERVAL '1 minute')"
            )

            parametros.extend(
                [
                    id_entidade,
                    nome_alias,
                    ultimo_api,
                ]
            )

            logging.info(
                "[API][INCREMENTAL] %s.%s | entidade=%s | alias=%s | "
                "checkpoint=%s | incremental direto com overlap=1min.",
                SCHEMA_DESTINO,
                tabela,
                id_entidade,
                nome_alias,
                ultimo_api,
            )

            continue

        # ============================================================
        # BOOTSTRAP API
        # ============================================================
        #
        # IMPORTANTE:
        # Não fazemos mais COUNT(*) no PostgreSQL Hub.
        #
        # O COUNT anterior podia varrer dezenas de milhões de linhas e
        # foi justamente o ponto em que o Orquestrador ficou parado.
        #
        # O Gold continua sendo consultado porque esse scan é rápido e
        # fornece o MAX(_ingestion_timestamp).
        # ============================================================

        qtd_gold, max_ingestion = obter_bootstrap_api(
            duck=duck,
            arquivo_parquet=arquivo_parquet,
            id_entidade=id_entidade,
            nome_alias=nome_alias,
        )

        # ============================================================
        # PRIMEIRA CARGA REAL / BOOTSTRAP SEGURO
        # ============================================================
        # Sem checkpoint em controle_api_dw, o Orquestrador NÃO pode
        # concluir que o histórico já existe no Hub apenas porque o
        # Central marcou carga_inicial_concluida.
        #
        # A ausência de checkpoint API significa bootstrap integral da
        # identidade. O UPSERT existente protege contra duplicidade.
        # O checkpoint do MAX(_ingestion_timestamp) do Gold só será
        # persistido depois do COMMIT bem-sucedido no Hub.
        # ============================================================

        if max_ingestion is not None:
            checkpoints_bootstrap.append(
                (
                    id_entidade,
                    nome_alias,
                    max_ingestion,
                )
            )

        logging.info(
            "[API][BOOTSTRAP] %s.%s | entidade=%s | alias=%s | "
            "Gold=%s | sem checkpoint API | UPSERT integral; "
            "checkpoint=%s somente apos COMMIT.",
            SCHEMA_DESTINO,
            tabela,
            id_entidade,
            nome_alias,
            qtd_gold,
            max_ingestion,
        )

        condicoes.append(
            "(CAST(_entity_id AS BIGINT) = ? "
            "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
        )

        parametros.extend(
            [
                id_entidade,
                nome_alias,
            ]
        )

    return condicoes, parametros, checkpoints_bootstrap

EVENT_TABLES_SYBASE = ("z_datalake", "z_datalake_itens")


def conectar_sybase_diagnostico(
    central: psycopg.Connection[Any],
    id_fonte: int,
) -> pyodbc.Connection:
    # Reutiliza exatamente a mesma origem de configuracao do Extrator:
    # fontes.id_referencia -> public.configuracao.
    with central.cursor() as cursor:
        cursor.execute(
            """
            SELECT id_referencia
            FROM public.fontes
            WHERE id = %s
              AND ativo IS TRUE
              AND UPPER(tipo_fonte) = 'ODBC';
            """,
            (id_fonte,),
        )
        fonte = cursor.fetchone()
        if fonte is None or fonte["id_referencia"] is None:
            raise RuntimeError(
                f"Fonte Sybase id={id_fonte} sem configuracao ODBC ativa."
            )

        cursor.execute(
            """
            SELECT
                ip_servidor, driver_odbc, nome_base, nome_server,
                usuario_db, senha_db, porta
            FROM public.configuracao
            WHERE id = %s
              AND ativo IS TRUE;
            """,
            (int(fonte["id_referencia"]),),
        )
        cfg = cursor.fetchone()

    # Encerra a transacao implicita de leitura do Central antes de abrir ODBC.
    central.commit()

    if cfg is None:
        raise RuntimeError(
            f"Configuracao ODBC da fonte id={id_fonte} nao encontrada."
        )

    conn_str = (
        f"DRIVER={{{cfg['driver_odbc']}}};"
        f"UID={cfg['usuario_db']};PWD={cfg['senha_db']};"
        f"ENG={cfg['nome_server']};DBN={cfg['nome_base']};"
        f"LINKS=tcpip(host={cfg['ip_servidor']};port={int(cfg['porta'])});"
    )
    return pyodbc.connect(conn_str, autocommit=False, timeout=30)


def diagnosticar_eventos_sincronizados_sybase(
    central: psycopg.Connection[Any],
    config: ConfigEntidade,
    nome_alias: str,
) -> dict[str, int | None] | None:
    # READ ONLY. Replica a identidade usada pelo Extrator nos logs:
    # a conexao define a base/fonte e NomeTabela define a entidade.
    # O Extrator NAO filtra CodEmpresa ao ler z_datalake/z_datalake_itens;
    # portanto este diagnostico tambem nao filtra, preservando inclusive NULL.
    conn: pyodbc.Connection | None = None
    try:
        conn = conectar_sybase_diagnostico(central, config.id_fonte)
        resultado: dict[str, int | None] = {}
        cursor = conn.cursor()

        for tabela_evento in EVENT_TABLES_SYBASE:
            cursor.execute(
                f"""SELECT TOP 1 "ID"
                    FROM "{tabela_evento}"
                    WHERE "NomeTabela" = ?
                      AND "Sincronizado" = 1
                    ORDER BY "ID" DESC""",
                (config.nome_tabela,),
            )
            linha = cursor.fetchone()
            resultado[tabela_evento] = int(linha[0]) if linha is not None else None

        # Nenhuma escrita e nenhum COMMIT no Sybase.
        return resultado
    except Exception:
        logging.exception(
            "[SYBASE][CHECKPOINT REPAIR][ATENCAO] entidade=%s | alias=%s | "
            "falha ao consultar eventos sincronizados na origem; "
            "checkpoint nao avancado; mantendo bootstrap seguro.",
            config.id_entidade, nome_alias,
        )
        return None
    finally:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()


def construir_filtro_sybase(
    identidades_sybase: list[dict[str, Any]],
    controle: ControleSincronizacao,
    configuracoes: dict[int, ConfigEntidade],
    central: psycopg.Connection[Any],
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
) -> tuple[
    list[str],
    list[Any],
    list[tuple[int, int, str, str, str, int, str | None]],
    list[tuple[int, int, str, str, str]],
]:
    inicio = time.perf_counter()
    condicoes: list[str] = []
    parametros: list[Any] = []
    checkpoints_bootstrap: list[tuple[int, int, str, str, str, int, str | None]] = []
    estados_sem_eventos: list[tuple[int, int, str, str, str]] = []

    for item in identidades_sybase:
        id_entidade_bruto = item.get("id_entidade")
        if id_entidade_bruto is None:
            continue

        id_entidade = int(id_entidade_bruto)
        config = configuracoes.get(id_entidade)
        if config is None:
            continue

        nome_alias = normalizar_alias(item.get("nome_alias"))
        if not nome_alias or eh_alias_api(nome_alias):
            continue

        if config.tipo_carga == "SNAPSHOT":
            if id_entidade in controle.cargas_concluidas:
                continue
            logging.info(
                "[SYBASE][SNAPSHOT] entidade=%s | alias=%s | carga inicial do snapshot.",
                id_entidade, nome_alias,
            )
            condicoes.append(
                "(CAST(_entity_id AS BIGINT) = ? "
                "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
            )
            parametros.extend([id_entidade, nome_alias])
            continue

        if id_entidade not in controle.cargas_concluidas:
            logging.info(
                "[SYBASE][PRIMEIRA_CARGA] entidade=%s | alias=%s | carga integral.",
                id_entidade, nome_alias,
            )
            condicoes.append(
                "(CAST(_entity_id AS BIGINT) = ? "
                "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
            )
            parametros.extend([id_entidade, nome_alias])
            continue

        checkpoints_entidade = {
            tabela_evento: ultimo_confirmado
            for (entidade_checkpoint, tabela_evento), ultimo_confirmado
            in controle.eventos_confirmados.items()
            if entidade_checkpoint == id_entidade
        }
        sem_historico_entidade = {
            tabela_evento
            for entidade_estado, tabela_evento in controle.eventos_sem_historico
            if entidade_estado == id_entidade
        }

        if not checkpoints_entidade and not sem_historico_entidade and not config.usar_trigger:
            logging.info(
                "[SYBASE][CARGA_INTEGRAL] entidade=%s | alias=%s | "
                "sem checkpoint e usar_trigger=FALSE; reparo de evento nao aplicado.",
                id_entidade, nome_alias,
            )
            condicoes.append(
                "(CAST(_entity_id AS BIGINT) = ? "
                "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
            )
            parametros.extend([id_entidade, nome_alias])
            continue

        # Caminho incremental normal. Checkpoints reais preservam exatamente
        # a semantica anterior. Se esta entidade nasceu do estado SEM_EVENTOS,
        # a event table ainda sem ID aceita qualquer primeiro evento real.
        if checkpoints_entidade:
            tabelas_conhecidas = set(checkpoints_entidade) | sem_historico_entidade
            for tabela_evento in sorted(tabelas_conhecidas):
                if tabela_evento in checkpoints_entidade:
                    ultimo_confirmado = checkpoints_entidade[tabela_evento]
                    logging.info(
                        "[SYBASE][INCREMENTAL] entidade=%s | alias=%s | "
                        "event_table=%s | checkpoint=%s.",
                        id_entidade, nome_alias, tabela_evento, ultimo_confirmado,
                    )
                    condicoes.append(
                        "(CAST(_entity_id AS BIGINT) = ? "
                        "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ? "
                        "AND CAST(_event_table AS VARCHAR) = ? "
                        "AND CAST(_event_id AS BIGINT) > ?)"
                    )
                    parametros.extend(
                        [id_entidade, nome_alias, tabela_evento, ultimo_confirmado]
                    )
                else:
                    logging.info(
                        "[SYBASE][SEM EVENTO SINCRONIZADO] entidade=%s | alias=%s | "
                        "event_table=%s | aguardando primeiro _event_id real.",
                        id_entidade, nome_alias, tabela_evento,
                    )
                    condicoes.append(
                        "(CAST(_entity_id AS BIGINT) = ? "
                        "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ? "
                        "AND CAST(_event_table AS VARCHAR) = ? "
                        "AND _event_id IS NOT NULL)"
                    )
                    parametros.extend([id_entidade, nome_alias, tabela_evento])
            continue

        if sem_historico_entidade:
            for tabela_evento in sorted(sem_historico_entidade):
                logging.info(
                    "[SYBASE][SEM EVENTO SINCRONIZADO] entidade=%s | alias=%s | "
                    "event_table=%s | aguardando primeiro _event_id real.",
                    id_entidade, nome_alias, tabela_evento,
                )
                condicoes.append(
                    "(CAST(_entity_id AS BIGINT) = ? "
                    "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ? "
                    "AND CAST(_event_table AS VARCHAR) = ? "
                    "AND _event_id IS NOT NULL)"
                )
                parametros.extend([id_entidade, nome_alias, tabela_evento])

            if set(EVENT_TABLES_SYBASE).issubset(sem_historico_entidade):
                continue

            # Estado parcial so ocorre quando outra event table tinha evento
            # sincronizado sem evidencia no Gold. Nesse caso, releitura e mais
            # segura do que presumir um checkpoint.
            logging.warning(
                "[SYBASE][CHECKPOINT REPAIR][ATENCAO] entidade=%s | alias=%s | "
                "estado SEM_EVENTOS parcial (%s); mantendo bootstrap seguro.",
                id_entidade, nome_alias, sorted(sem_historico_entidade),
            )
            condicoes.append(
                "(CAST(_entity_id AS BIGINT) = ? "
                "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
            )
            parametros.extend([id_entidade, nome_alias])
            continue

        # Sem qualquer checkpoint/estado: primeiro tenta reparar exclusivamente
        # pelo Gold, mantendo a logica ja existente.
        linhas_checkpoint = duck.execute(
            """
            SELECT
                CAST(_event_table AS VARCHAR) AS tabela_evento,
                MAX(CAST(_event_id AS BIGINT)) AS max_event_id,
                ARG_MAX(CAST(_batch_id AS VARCHAR), CAST(_event_id AS BIGINT)) AS ultimo_batch_id,
                ARG_MAX(CAST(_entity AS VARCHAR), CAST(_event_id AS BIGINT)) AS nome_entidade
            FROM read_parquet(?)
            WHERE CAST(_entity_id AS BIGINT) = ?
              AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?
              AND _event_table IS NOT NULL
              AND _event_id IS NOT NULL
            GROUP BY CAST(_event_table AS VARCHAR)
            """,
            [str(arquivo_parquet), id_entidade, nome_alias],
        ).fetchall()

        if linhas_checkpoint:
            for tabela_evento, max_event_id, ultimo_batch_id, nome_entidade in linhas_checkpoint:
                checkpoints_bootstrap.append(
                    (
                        id_entidade, config.id_fonte, nome_alias,
                        str(nome_entidade or config.nome_tabela),
                        str(tabela_evento), int(max_event_id),
                        str(ultimo_batch_id) if ultimo_batch_id is not None else None,
                    )
                )
                logging.info(
                    "[SYBASE][CHECKPOINT REPAIR] entidade=%s | alias=%s | "
                    "event_table=%s | high-water mark Gold=%s | "
                    "checkpoint deferido ate COMMIT do Hub.",
                    id_entidade, nome_alias, tabela_evento, max_event_id,
                )
        else:
            logging.info(
                "[SYBASE][CHECKPOINT REPAIR] entidade=%s | alias=%s | "
                "nenhum _event_id/_event_table correspondente no Gold; "
                "consultando somente estado sincronizado da origem.",
                id_entidade, nome_alias,
            )
            diagnostico = diagnosticar_eventos_sincronizados_sybase(
                central=central, config=config, nome_alias=nome_alias
            )
            if diagnostico is not None:
                for tabela_evento in EVENT_TABLES_SYBASE:
                    maior_id = diagnostico.get(tabela_evento)
                    if maior_id is None:
                        estados_sem_eventos.append(
                            (
                                id_entidade, config.id_fonte, nome_alias,
                                config.nome_tabela, tabela_evento,
                            )
                        )
                        logging.info(
                            "[SYBASE][SEM EVENTO SINCRONIZADO] entidade=%s | "
                            "alias=%s | event_table=%s | nenhum evento ja "
                            "sincronizado encontrado na origem; eventos pendentes "
                            "permanecem sob responsabilidade do Extrator; "
                            "estado deferido ate COMMIT do Hub.",
                            id_entidade, nome_alias, tabela_evento,
                        )
                    else:
                        logging.warning(
                            "[SYBASE][CHECKPOINT REPAIR][ATENCAO] entidade=%s | "
                            "alias=%s | event_table=%s | maior_id_sincronizado=%s | "
                            "origem possui evento sincronizado mas Gold nao possui "
                            "high-water mark; checkpoint nao avancado.",
                            id_entidade, nome_alias, tabela_evento, maior_id,
                        )

        # Esta execucao ainda faz bootstrap integral. Qualquer checkpoint Gold ou
        # estado SEM_EVENTOS so e persistido depois do COMMIT do Hub.
        logging.info(
            "[SYBASE][PRIMEIRA_CARGA] entidade=%s | alias=%s | "
            "sem checkpoint de evento; bootstrap integral desta execucao.",
            id_entidade, nome_alias,
        )
        condicoes.append(
            "(CAST(_entity_id AS BIGINT) = ? "
            "AND UPPER(TRIM(CAST(nome_alias AS VARCHAR))) = ?)"
        )
        parametros.extend([id_entidade, nome_alias])

    logging.info(
        "[PERF] SYBASE resumo/scan | filtros por checkpoint/reparo em "
        "%.4f segundos | %s identidade(s).",
        time.perf_counter() - inicio, len(identidades_sybase),
    )
    return condicoes, parametros, checkpoints_bootstrap, estados_sem_eventos

def construir_filtro_pendentes(
    identidades_sybase: list[dict[str, Any]],
    identidades_api: list[dict[str, Any]],
    controle: ControleSincronizacao,
    configuracoes: dict[int, ConfigEntidade],
    hub_dw: psycopg.Connection[Any],
    central: psycopg.Connection[Any],
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
    tabela: str,
) -> tuple[
    str | None,
    list[Any],
    list[tuple[int, str, Any]],
    list[tuple[int, int, str, str, str, int, str | None]],
    list[tuple[int, int, str, str, str]],
]:
    # Mantem os dois fluxos explicitamente independentes.
    (
        cond_sybase, par_sybase, checkpoints_sybase_bootstrap,
        estados_sybase_sem_eventos,
    ) = construir_filtro_sybase(
        identidades_sybase, controle, configuracoes, central, duck, arquivo_parquet
    )
    inicio_api = time.perf_counter()
    cond_api, par_api, checkpoints_api_bootstrap = construir_filtro_api(
        identidades_api, controle, configuracoes, hub_dw, central,
        duck, arquivo_parquet, tabela
    )
    logging.info(
        "[PERF] API filtros/bootstrap | %s | %.4f segundos.",
        tabela,
        time.perf_counter() - inicio_api,
    )
    condicoes = cond_sybase + cond_api
    parametros = par_sybase + par_api
    if not condicoes:
        return (
            None, [], checkpoints_api_bootstrap, checkpoints_sybase_bootstrap,
            estados_sybase_sem_eventos,
        )
    return (
        " OR ".join(condicoes), parametros,
        checkpoints_api_bootstrap, checkpoints_sybase_bootstrap,
        estados_sybase_sem_eventos,
    )


def iterar_linhas_pendentes(
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
    colunas_postgres: list[str],
    controle: ControleSincronizacao,
    configuracoes: dict[int, ConfigEntidade],
    hub_dw: psycopg.Connection[Any],
    central: psycopg.Connection[Any],
    tabela: str,
    checkpoints_api_bootstrap: list[tuple[int, str, Any]],
    checkpoints_sybase_bootstrap: list[tuple[int, int, str, str, str, int, str | None]],
    estados_sybase_sem_eventos: list[tuple[int, int, str, str, str]],
) -> Iterator[
    tuple[tuple[Any, ...], ConfirmacaoEntidade]
]:
    logging.info("[SYBASE] %s | montando filtros pelos checkpoints do Central.", tabela)
    logging.info("[API] %s | obtendo identidades pelo Central.", tabela)

    identidades_hub = obter_identidades_hub_central(central, tabela)
    # O SELECT acima abre transacao implicita no psycopg. Encerra a leitura
    # antes de qualquer bootstrap/checkpoint para preservar COMMIT independente.
    central.commit()

    identidades_sybase, identidades_api = separar_identidades_hub(
        identidades_hub
    )

    logging.info(
        "[PERF] Identidades API pelo Central | %s | %s identidade(s).",
        tabela,
        len(identidades_api),
    )

    (
        filtro, parametros, checkpoints_bootstrap,
        checkpoints_sybase_reparo, estados_sem_eventos,
    ) = construir_filtro_pendentes(
        identidades_sybase,
        identidades_api,
        controle,
        configuracoes,
        hub_dw,
        central,
        duck,
        arquivo_parquet,
        tabela,
    )
    checkpoints_api_bootstrap.extend(checkpoints_bootstrap)
    checkpoints_sybase_bootstrap.extend(checkpoints_sybase_reparo)
    estados_sybase_sem_eventos.extend(estados_sem_eventos)

    if filtro is None:
        return

    colunas_leitura = list(
        dict.fromkeys(
            colunas_postgres
            + [
                "_source_id",
                "_entity_id",
                "_entity",
                "_event_table",
                "_operation",
                "_event_id",
                "_batch_id",
                "_ingestion_timestamp",
            ]
        )
    )

    selecao = ", ".join(
        _qid(coluna)
        for coluna in colunas_leitura
    )

    consulta = (
        f"SELECT {selecao} "
        "FROM read_parquet(?) "
        f"WHERE {filtro}"
    )

    inicio_incremental = time.perf_counter()
    cursor_incremental = duck.execute(
        consulta,
        [str(arquivo_parquet), *parametros],
    )
    leitor_lotes = cursor_incremental.fetch_record_batch(TAMANHO_LOTE)
    primeiro_lote = True

    for lote in leitor_lotes:
        if primeiro_lote:
            logging.info(
                "[PERF] Primeiro lote incremental | %s | %.4f segundos | %s linha(s).",
                tabela,
                time.perf_counter() - inicio_incremental,
                lote.num_rows,
            )
            primeiro_lote = False

        dados = lote.to_pydict()

        for indice in range(lote.num_rows):
            id_fonte_bruto = normalizar_valor(
                dados["_source_id"][indice]
            )
            id_entidade_bruto = normalizar_valor(
                dados["_entity_id"][indice]
            )

            if id_fonte_bruto is None or id_entidade_bruto is None:
                raise RuntimeError(
                    "Registro sem _source_id ou _entity_id."
                )

            id_fonte = int(id_fonte_bruto)
            id_entidade = int(id_entidade_bruto)

            nome_entidade = str(
                normalizar_valor(dados["_entity"][indice]) or ""
            )
            nome_alias = str(
                normalizar_valor(dados["nome_alias"][indice]) or ""
            )

            tabela_evento_valor = normalizar_valor(
                dados["_event_table"][indice]
            )
            tabela_evento = (
                str(tabela_evento_valor)
                if tabela_evento_valor is not None
                else None
            )

            event_id_valor = normalizar_valor(
                dados["_event_id"][indice]
            )
            event_id = (
                int(event_id_valor)
                if event_id_valor is not None
                else None
            )

            config = configuracoes.get(id_entidade)
            if config is None:
                continue

            primeira_carga = (
                id_entidade not in controle.cargas_concluidas
            )

            operacao = normalizar_valor(
                dados["_operation"][indice]
            )

            linha_destino = tuple(
                normalizar_valor(dados[coluna][indice])
                for coluna in colunas_postgres
            ) + (operacao,)

            confirmacao = ConfirmacaoEntidade(
                id_entidade=id_entidade,
                id_fonte=id_fonte,
                nome_alias=nome_alias,
                nome_entidade=nome_entidade,
                primeira_carga=primeira_carga,
            )

            alias_normalizado = normalizar_alias(nome_alias)

            if eh_alias_api(alias_normalizado):
                ingestion_valor = normalizar_valor(
                    dados["_ingestion_timestamp"][indice]
                )
                confirmacao.ultimo_ingestion_ts = ingestion_valor

            if (
                not eh_alias_api(alias_normalizado)
                and tabela_evento is not None
                and event_id is not None
            ):
                batch_valor = normalizar_valor(
                    dados["_batch_id"][indice]
                )
                confirmacao.eventos[tabela_evento] = (
                    event_id,
                    (
                        str(batch_valor)
                        if batch_valor is not None
                        else None
                    ),
                )

            yield linha_destino, confirmacao

    if primeiro_lote:
        logging.info(
            "[PERF] Primeiro lote incremental | %s | sem linhas | %.4f segundos.",
            tabela,
            time.perf_counter() - inicio_incremental,
        )


def consolidar_confirmacao(
    destino: dict[int, ConfirmacaoEntidade],
    nova: ConfirmacaoEntidade,
) -> None:
    atual = destino.get(nova.id_entidade)

    if atual is None:
        destino[nova.id_entidade] = nova
        return

    if (
        atual.id_fonte != nova.id_fonte
        or atual.nome_alias != nova.nome_alias
        or atual.nome_entidade != nova.nome_entidade
    ):
        raise RuntimeError(
            "Metadados divergentes para o mesmo _entity_id "
            f"{nova.id_entidade}."
        )

    atual.primeira_carga = (
        atual.primeira_carga or nova.primeira_carga
    )

    if nova.ultimo_ingestion_ts is not None:
        if (
            atual.ultimo_ingestion_ts is None
            or nova.ultimo_ingestion_ts > atual.ultimo_ingestion_ts
        ):
            atual.ultimo_ingestion_ts = nova.ultimo_ingestion_ts

    for tabela_evento, novo_evento in nova.eventos.items():
        evento_atual = atual.eventos.get(tabela_evento)

        if evento_atual is None or novo_evento[0] > evento_atual[0]:
            atual.eventos[tabela_evento] = novo_evento


def validar_chaves_temporaria(
    cursor: psycopg.Cursor[Any],
    nome_temporaria: str,
    chaves: list[str],
) -> None:
    condicao_nulos = sql.SQL(" OR ").join(
        sql.SQL("{} IS NULL").format(sql.Identifier(chave))
        for chave in chaves
    )

    consulta_nulos = sql.SQL(
        "SELECT COUNT(*) AS quantidade "
        "FROM {} WHERE {}"
    ).format(
        sql.Identifier(nome_temporaria),
        condicao_nulos,
    )

    cursor.execute(consulta_nulos)
    quantidade_nulos = int(cursor.fetchone()["quantidade"])

    if quantidade_nulos:
        raise RuntimeError(
            f"A tabela temporaria {nome_temporaria} possui "
            f"{quantidade_nulos} registro(s) com chave de UPSERT nula."
        )

    consulta_duplicados = sql.SQL(
        "SELECT COUNT(*) AS quantidade FROM ("
        "SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1"
        ") AS duplicados"
    ).format(
        sql.SQL(", ").join(
            sql.Identifier(chave)
            for chave in chaves
        ),
        sql.Identifier(nome_temporaria),
        sql.SQL(", ").join(
            sql.Identifier(chave)
            for chave in chaves
        ),
    )

    cursor.execute(consulta_duplicados)
    quantidade_duplicados = int(
        cursor.fetchone()["quantidade"]
    )

    if quantidade_duplicados:
        raise RuntimeError(
            f"A tabela temporaria {nome_temporaria} possui "
            f"{quantidade_duplicados} chave(s) duplicada(s). "
            "O UPSERT foi interrompido para evitar resultado ambiguo."
        )


def carregar_parquet_na_tabela(
    conexao: psycopg.Connection[Any],
    duck: duckdb.DuckDBPyConnection,
    arquivo_parquet: Path,
    tabela: str,
    controle: ControleSincronizacao,
    configuracoes: dict[int, ConfigEntidade],
    central: psycopg.Connection[Any],
) -> ResultadoCarga:
    parquet = pq.ParquetFile(arquivo_parquet)
    colunas_parquet = parquet.schema_arrow.names

    colunas_postgres = listar_colunas_postgres(
        conexao,
        SCHEMA_DESTINO,
        tabela,
    )

    if not colunas_postgres:
        raise RuntimeError(
            f"Tabela {SCHEMA_DESTINO}.{tabela} nao encontrada."
        )

    validar_colunas(
        arquivo_parquet,
        colunas_parquet,
        colunas_postgres,
        tabela,
    )

    chaves_upsert = CHAVES_UPSERT.get(tabela)

    if not chaves_upsert:
        raise RuntimeError(
            f"Chaves de UPSERT nao configuradas para "
            f"{SCHEMA_DESTINO}.{tabela}."
        )

    colunas_inexistentes = [
        coluna
        for coluna in chaves_upsert
        if coluna not in colunas_postgres
    ]

    if colunas_inexistentes:
        raise RuntimeError(
            f"Chaves de UPSERT inexistentes em "
            f"{SCHEMA_DESTINO}.{tabela}: "
            + ", ".join(colunas_inexistentes)
        )

    garantir_indice_unico(
        conexao,
        SCHEMA_DESTINO,
        tabela,
        chaves_upsert,
    )

    colunas_temporaria = colunas_postgres + [COLUNA_OPERACAO]

    colunas_atualizacao = [
        coluna
        for coluna in colunas_postgres
        if coluna not in chaves_upsert
    ]

    nome_temporaria = f"tmp_pombo_{tabela}"

    comando_criar_temporaria = sql.SQL(
        "CREATE TEMP TABLE {} "
        "(LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP"
    ).format(
        sql.Identifier(nome_temporaria),
        sql.Identifier(SCHEMA_DESTINO),
        sql.Identifier(tabela),
    )

    comando_adicionar_operacao = sql.SQL(
        "ALTER TABLE {} ADD COLUMN {} TEXT"
    ).format(
        sql.Identifier(nome_temporaria),
        sql.Identifier(COLUNA_OPERACAO),
    )

    comando_copy = sql.SQL(
        "COPY {} ({}) FROM STDIN"
    ).format(
        sql.Identifier(nome_temporaria),
        sql.SQL(", ").join(
            sql.Identifier(coluna)
            for coluna in colunas_temporaria
        ),
    )

    condicao_delete = montar_condicao_delete(chaves_upsert)

    comando_delete = sql.SQL(
        "DELETE FROM {}.{} AS destino "
        "USING {} AS origem "
        "WHERE UPPER(TRIM(COALESCE(origem.{}, ''))) "
        "IN ('DELETE', 'DELETED', 'D') "
        "AND {}"
    ).format(
        sql.Identifier(SCHEMA_DESTINO),
        sql.Identifier(tabela),
        sql.Identifier(nome_temporaria),
        sql.Identifier(COLUNA_OPERACAO),
        condicao_delete,
    )

    if colunas_atualizacao:
        atribuicoes = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(coluna),
                sql.Identifier(coluna),
            )
            for coluna in colunas_atualizacao
        )

        condicao_alteracao = sql.SQL(" OR ").join(
            sql.SQL(
                "destino.{} IS DISTINCT FROM EXCLUDED.{}"
            ).format(
                sql.Identifier(coluna),
                sql.Identifier(coluna),
            )
            for coluna in colunas_atualizacao
        )

        comando_upsert = sql.SQL(
            "INSERT INTO {}.{} AS destino ({}) "
            "SELECT {} FROM {} "
            "WHERE UPPER(TRIM(COALESCE({}, ''))) "
            "NOT IN ('DELETE', 'DELETED', 'D') "
            "ON CONFLICT ({}) DO UPDATE SET {} "
            "WHERE {}"
        ).format(
            sql.Identifier(SCHEMA_DESTINO),
            sql.Identifier(tabela),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in colunas_postgres
            ),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in colunas_postgres
            ),
            sql.Identifier(nome_temporaria),
            sql.Identifier(COLUNA_OPERACAO),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in chaves_upsert
            ),
            atribuicoes,
            condicao_alteracao,
        )
    else:
        comando_upsert = sql.SQL(
            "INSERT INTO {}.{} AS destino ({}) "
            "SELECT {} FROM {} "
            "WHERE UPPER(TRIM(COALESCE({}, ''))) "
            "NOT IN ('DELETE', 'DELETED', 'D') "
            "ON CONFLICT ({}) DO NOTHING"
        ).format(
            sql.Identifier(SCHEMA_DESTINO),
            sql.Identifier(tabela),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in colunas_postgres
            ),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in colunas_postgres
            ),
            sql.Identifier(nome_temporaria),
            sql.Identifier(COLUNA_OPERACAO),
            sql.SQL(", ").join(
                sql.Identifier(coluna)
                for coluna in chaves_upsert
            ),
        )

    total_enviado = 0
    confirmacoes: dict[int, ConfirmacaoEntidade] = {}
    checkpoints_api_bootstrap: list[tuple[int, str, Any]] = []
    checkpoints_sybase_bootstrap: list[tuple[int, int, str, str, str, int, str | None]] = []
    estados_sybase_sem_eventos: list[tuple[int, int, str, str, str]] = []

    with conexao.cursor() as cursor:
        cursor.execute(comando_criar_temporaria)
        cursor.execute(comando_adicionar_operacao)

        with cursor.copy(comando_copy) as copia:
            for linha, confirmacao in iterar_linhas_pendentes(
                duck=duck,
                arquivo_parquet=arquivo_parquet,
                colunas_postgres=colunas_postgres,
                controle=controle,
                configuracoes=configuracoes,
                hub_dw=conexao,
                central=central,
                tabela=tabela,
                checkpoints_api_bootstrap=checkpoints_api_bootstrap,
                checkpoints_sybase_bootstrap=checkpoints_sybase_bootstrap,
                estados_sybase_sem_eventos=estados_sybase_sem_eventos,
            ):
                copia.write_row(linha)
                total_enviado += 1
                consolidar_confirmacao(
                    confirmacoes,
                    confirmacao,
                )

        if total_enviado == 0:
            return ResultadoCarga(
                enviados=0,
                upserts=0,
                excluidos=0,
                ignorados=0,
                confirmacoes=(),
                checkpoints_api_bootstrap=tuple(checkpoints_api_bootstrap),
                checkpoints_sybase_bootstrap=tuple(checkpoints_sybase_bootstrap),
                estados_sybase_sem_eventos=tuple(estados_sybase_sem_eventos),
            )

        validar_chaves_temporaria(
            cursor,
            nome_temporaria,
            chaves_upsert,
        )

        cursor.execute(comando_delete)
        total_excluido = max(cursor.rowcount, 0)

        cursor.execute(comando_upsert)
        total_upsert = max(cursor.rowcount, 0)

    total_ignorado = max(
        total_enviado - total_excluido - total_upsert,
        0,
    )

    return ResultadoCarga(
        enviados=total_enviado,
        upserts=total_upsert,
        excluidos=total_excluido,
        ignorados=total_ignorado,
        confirmacoes=tuple(confirmacoes.values()),
        checkpoints_api_bootstrap=tuple(checkpoints_api_bootstrap),
        checkpoints_sybase_bootstrap=tuple(checkpoints_sybase_bootstrap),
        estados_sybase_sem_eventos=tuple(estados_sybase_sem_eventos),
    )



def atualizar_checkpoints_sybase_bootstrap(
    central: psycopg.Connection[Any],
    controle: ControleSincronizacao,
    checkpoints: tuple[tuple[int, int, str, str, str, int, str | None], ...],
) -> None:
    if not checkpoints:
        return

    with central.transaction():
        with central.cursor() as cursor:
            for (
                id_entidade, id_fonte, nome_alias, nome_entidade,
                tabela_evento, event_id, batch_id,
            ) in checkpoints:
                cursor.execute(
                    """
                    INSERT INTO public.controle_evento_dw
                    (
                        id_entidade, id_fonte, nome_alias, nome_entidade,
                        tabela_evento, ultimo_event_id_confirmado,
                        ultimo_batch_id, data_ultima_confirmacao, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'SUCESSO')
                    ON CONFLICT (id_entidade, tabela_evento)
                    DO UPDATE SET
                        id_fonte = EXCLUDED.id_fonte,
                        nome_alias = EXCLUDED.nome_alias,
                        nome_entidade = EXCLUDED.nome_entidade,
                        ultimo_event_id_confirmado = GREATEST(
                            COALESCE(
                                public.controle_evento_dw.ultimo_event_id_confirmado,
                                -1
                            ),
                            EXCLUDED.ultimo_event_id_confirmado
                        ),
                        ultimo_batch_id = EXCLUDED.ultimo_batch_id,
                        data_ultima_confirmacao = CURRENT_TIMESTAMP,
                        status = 'SUCESSO';
                    """,
                    (
                        id_entidade, id_fonte, nome_alias, nome_entidade,
                        tabela_evento, event_id, batch_id,
                    ),
                )
                chave = (id_entidade, tabela_evento)
                controle.eventos_confirmados[chave] = max(
                    controle.eventos_confirmados.get(chave, -1), event_id
                )
                controle.eventos_sem_historico.discard(chave)
                logging.info(
                    "[SYBASE][CHECKPOINT REPAIR] entidade=%s | alias=%s | "
                    "event_table=%s | checkpoint=%s | "
                    "checkpoint inicializado apos confirmacao do Hub.",
                    id_entidade, nome_alias, tabela_evento, event_id,
                )


def atualizar_estados_sybase_sem_eventos(
    central: psycopg.Connection[Any],
    controle: ControleSincronizacao,
    estados: tuple[tuple[int, int, str, str, str], ...],
) -> None:
    if not estados:
        return

    with central.transaction():
        with central.cursor() as cursor:
            for id_entidade, id_fonte, nome_alias, nome_entidade, tabela_evento in estados:
                # Nao inventa event_id. O NULL representa explicitamente que
                # ainda nao havia evento sincronizado comprovado para esta table.
                cursor.execute(
                    """
                    INSERT INTO public.controle_evento_dw
                    (
                        id_entidade, id_fonte, nome_alias, nome_entidade,
                        tabela_evento, ultimo_event_id_confirmado,
                        ultimo_batch_id, data_ultima_confirmacao, status
                    )
                    VALUES (%s, %s, %s, %s, %s, NULL, NULL, CURRENT_TIMESTAMP, 'SEM_EVENTOS')
                    ON CONFLICT (id_entidade, tabela_evento)
                    DO UPDATE SET
                        id_fonte = EXCLUDED.id_fonte,
                        nome_alias = EXCLUDED.nome_alias,
                        nome_entidade = EXCLUDED.nome_entidade,
                        data_ultima_confirmacao = CURRENT_TIMESTAMP,
                        status = CASE
                            WHEN public.controle_evento_dw.ultimo_event_id_confirmado IS NULL
                            THEN 'SEM_EVENTOS'
                            ELSE public.controle_evento_dw.status
                        END;
                    """,
                    (id_entidade, id_fonte, nome_alias, nome_entidade, tabela_evento),
                )
                chave = (id_entidade, tabela_evento)
                if chave not in controle.eventos_confirmados:
                    controle.eventos_sem_historico.add(chave)
                logging.info(
                    "[SYBASE][SEM EVENTO SINCRONIZADO] entidade=%s | alias=%s | "
                    "event_table=%s | estado SEM_EVENTOS confirmado apos COMMIT do Hub; "
                    "nenhum _event_id foi inventado.",
                    id_entidade, nome_alias, tabela_evento,
                )


def atualizar_checkpoints_api_bootstrap(
    central: psycopg.Connection[Any],
    controle: ControleSincronizacao,
    checkpoints: tuple[tuple[int, str, Any], ...],
) -> None:
    if not checkpoints:
        return

    with central.transaction():
        with central.cursor() as cursor:
            for id_entidade, nome_alias, max_ingestion in checkpoints:
                cursor.execute(
                    """
                    INSERT INTO public.controle_api_dw
                        (id_entidade, nome_alias, ultimo_ingestion_ts,
                         data_ultima_confirmacao, status)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'SUCESSO')
                    ON CONFLICT (id_entidade, nome_alias)
                    DO UPDATE SET
                        ultimo_ingestion_ts = GREATEST(
                            public.controle_api_dw.ultimo_ingestion_ts,
                            EXCLUDED.ultimo_ingestion_ts
                        ),
                        data_ultima_confirmacao = CURRENT_TIMESTAMP,
                        status = 'SUCESSO';
                    """,
                    (id_entidade, nome_alias, max_ingestion),
                )
                controle.api_confirmados[(id_entidade, nome_alias)] = max_ingestion
                logging.info(
                    "[API][BOOTSTRAP] entidade=%s | alias=%s | checkpoint=%s | "
                    "checkpoint confirmado apos COMMIT do Hub.",
                    id_entidade, nome_alias, max_ingestion,
                )

def atualizar_controles_central(
    central: psycopg.Connection[Any],
    controle: ControleSincronizacao,
    confirmacoes: tuple[ConfirmacaoEntidade, ...],
) -> None:
    if not confirmacoes:
        return

    with central.transaction():
        with central.cursor() as cursor:
            for confirmacao in confirmacoes:
                cursor.execute(
                    """
                    INSERT INTO public.controle_carga_dw
                    (
                        id_entidade,
                        id_fonte,
                        nome_alias,
                        nome_entidade,
                        carga_inicial_concluida,
                        data_carga_inicial,
                        data_ultima_confirmacao,
                        status
                    )
                    VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s,
                        TRUE,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        'SUCESSO'
                    )
                    ON CONFLICT (id_entidade)
                    DO UPDATE SET
                        id_fonte = EXCLUDED.id_fonte,
                        nome_alias = EXCLUDED.nome_alias,
                        nome_entidade = EXCLUDED.nome_entidade,
                        carga_inicial_concluida = TRUE,
                        data_carga_inicial = COALESCE(
                            public.controle_carga_dw.data_carga_inicial,
                            EXCLUDED.data_carga_inicial
                        ),
                        data_ultima_confirmacao =
                            EXCLUDED.data_ultima_confirmacao,
                        status = 'SUCESSO';
                    """,
                    (
                        confirmacao.id_entidade,
                        confirmacao.id_fonte,
                        confirmacao.nome_alias,
                        confirmacao.nome_entidade,
                    ),
                )

                controle.cargas_concluidas.add(
                    confirmacao.id_entidade
                )

                cursor.execute(
                    """
                    UPDATE public.configuracao_entidades
                    SET
                        carga_inicial_concluida = TRUE,
                        data_carga_inicial = COALESCE(
                            data_carga_inicial,
                            CURRENT_TIMESTAMP
                        ),
                        ultima_execucao_status = 'SUCESSO'
                    WHERE id = %s;
                    """,
                    (confirmacao.id_entidade,),
                )

                alias_normalizado = normalizar_alias(confirmacao.nome_alias)
                if (
                    eh_alias_api(alias_normalizado)
                    and confirmacao.ultimo_ingestion_ts is not None
                ):
                    cursor.execute(
                        """
                        INSERT INTO public.controle_api_dw
                        (
                            id_entidade,
                            nome_alias,
                            ultimo_ingestion_ts,
                            data_ultima_confirmacao,
                            status
                        )
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'SUCESSO')
                        ON CONFLICT (id_entidade, nome_alias)
                        DO UPDATE SET
                            ultimo_ingestion_ts = GREATEST(
                                public.controle_api_dw.ultimo_ingestion_ts,
                                EXCLUDED.ultimo_ingestion_ts
                            ),
                            data_ultima_confirmacao = CURRENT_TIMESTAMP,
                            status = 'SUCESSO';
                        """,
                        (
                            confirmacao.id_entidade,
                            alias_normalizado,
                            confirmacao.ultimo_ingestion_ts,
                        ),
                    )
                    controle.api_confirmados[(
                        confirmacao.id_entidade, alias_normalizado
                    )] = confirmacao.ultimo_ingestion_ts

                for (
                    tabela_evento,
                    (event_id, batch_id),
                ) in confirmacao.eventos.items():
                    cursor.execute(
                        """
                        INSERT INTO public.controle_evento_dw
                        (
                            id_entidade,
                            id_fonte,
                            nome_alias,
                            nome_entidade,
                            tabela_evento,
                            ultimo_event_id_confirmado,
                            ultimo_batch_id,
                            data_ultima_confirmacao,
                            status
                        )
                        VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            CURRENT_TIMESTAMP,
                            'SUCESSO'
                        )
                        ON CONFLICT
                        (
                            id_entidade,
                            tabela_evento
                        )
                        DO UPDATE SET
                            id_fonte = EXCLUDED.id_fonte,
                            nome_alias = EXCLUDED.nome_alias,
                            nome_entidade = EXCLUDED.nome_entidade,
                            ultimo_event_id_confirmado = GREATEST(
                                COALESCE(
                                    public.controle_evento_dw
                                        .ultimo_event_id_confirmado,
                                    -1
                                ),
                                EXCLUDED.ultimo_event_id_confirmado
                            ),
                            ultimo_batch_id =
                                EXCLUDED.ultimo_batch_id,
                            data_ultima_confirmacao =
                                EXCLUDED.data_ultima_confirmacao,
                            status = 'SUCESSO';
                        """,
                        (
                            confirmacao.id_entidade,
                            confirmacao.id_fonte,
                            confirmacao.nome_alias,
                            confirmacao.nome_entidade,
                            tabela_evento,
                            event_id,
                            batch_id,
                        ),
                    )

                    chave = (
                        confirmacao.id_entidade,
                        tabela_evento,
                    )
                    controle.eventos_confirmados[chave] = max(
                        controle.eventos_confirmados.get(chave, -1),
                        event_id,
                    )
                    controle.eventos_sem_historico.discard(chave)


def validar_arquivos(
    pasta_gold: Path,
    tabelas_hub: list[str],
) -> None:
    if not pasta_gold.exists():
        raise FileNotFoundError(
            f"Pasta Gold nao encontrada ou sem acesso: {pasta_gold}"
        )

    ausentes = [
        f"{tabela}.parquet"
        for tabela in tabelas_hub
        if not (pasta_gold / f"{tabela}.parquet").is_file()
    ]

    if ausentes:
        logging.warning(
            "Parquets ausentes (tabelas ignoradas neste ciclo): %s",
            ", ".join(ausentes),
        )


def executar_pipeline() -> None:
    inicio_pipeline = time.perf_counter()

    configurar_logging()
    logging.info("Iniciando Pombo Orquestrador DW.")

    config_central, config_hub = carregar_configuracoes()

    total_upserts = 0
    total_excluidos = 0
    total_ignorados = 0
    total_enviados = 0

    duck = duckdb.connect()

    try:
        with conectar_postgres(
            config_central,
            "Pombo_Orquestrador_Controle",
        ) as central, conectar_postgres(
            config_hub,
            "Pombo_Orquestrador_DW",
        ) as hub_dw:
            controle = carregar_controles(central)
            configuracoes_entidades = carregar_configuracoes_entidades(
                central
            )
            tabelas_hub = listar_tabelas_hub_api_primeiro(
                central,
                configuracoes_entidades,
            )
            pasta_gold = obter_pasta_gold(central)

            # Os SELECTs acima abrem uma transacao implicita no psycopg.
            # Encerra agora para que cada atualizacao de checkpoint abaixo
            # tenha COMMIT independente e fique visivel imediatamente.
            central.commit()

            validar_arquivos(
                pasta_gold,
                tabelas_hub,
            )

            logging.info("Pasta Gold: %s", pasta_gold)
            logging.info(
                "Conectado ao central %s:%s/%s e ao DW %s:%s/%s.",
                config_central.host,
                config_central.port,
                config_central.dbname,
                config_hub.host,
                config_hub.port,
                config_hub.dbname,
            )

            for tabela in tabelas_hub:
                nome_arquivo = f"{tabela}.parquet"
                arquivo = pasta_gold / nome_arquivo

                if not arquivo.is_file():
                    logging.warning(
                        "%s ausente; %s.%s ignorada neste ciclo.",
                        nome_arquivo,
                        SCHEMA_DESTINO,
                        tabela,
                    )
                    continue

                inicio_tabela = time.perf_counter()

                logging.info(
                    "Verificando %s -> %s.%s",
                    arquivo.name,
                    SCHEMA_DESTINO,
                    tabela,
                )

                try:
                    # DELETE e UPSERT ficam na mesma transacao do hub_dw.
                    with hub_dw.transaction():
                        resultado = carregar_parquet_na_tabela(
                            conexao=hub_dw,
                            duck=duck,
                            arquivo_parquet=arquivo,
                            tabela=tabela,
                            controle=controle,
                            configuracoes=configuracoes_entidades,
                            central=central,
                        )

                    # O controle avanca somente depois do COMMIT no hub_dw.
                    atualizar_checkpoints_sybase_bootstrap(
                        central=central,
                        controle=controle,
                        checkpoints=resultado.checkpoints_sybase_bootstrap,
                    )

                    atualizar_estados_sybase_sem_eventos(
                        central=central,
                        controle=controle,
                        estados=resultado.estados_sybase_sem_eventos,
                    )

                    atualizar_checkpoints_api_bootstrap(
                        central=central,
                        controle=controle,
                        checkpoints=resultado.checkpoints_api_bootstrap,
                    )

                    # Como a transacao inicial da central ja foi encerrada,
                    # este transaction() faz COMMIT por entidade.
                    atualizar_controles_central(
                        central=central,
                        controle=controle,
                        confirmacoes=resultado.confirmacoes,
                    )

                    duracao = time.perf_counter() - inicio_tabela

                    total_enviados += resultado.enviados
                    total_upserts += resultado.upserts
                    total_excluidos += resultado.excluidos
                    total_ignorados += resultado.ignorados

                    logging.info(
                        "SUCESSO | %s.%s | pendentes=%s | "
                        "upserts=%s | excluidos=%s | "
                        "sem alteracao=%s | %.2f segundos",
                        SCHEMA_DESTINO,
                        tabela,
                        resultado.enviados,
                        resultado.upserts,
                        resultado.excluidos,
                        resultado.ignorados,
                        duracao,
                    )

                except Exception:
                    logging.exception(
                        "ERRO ao processar %s em %s.%s. "
                        "O checkpoint nao foi avancado.",
                        arquivo.name,
                        SCHEMA_DESTINO,
                        tabela,
                    )
                    raise

        duracao_total = time.perf_counter() - inicio_pipeline
        total_afetado = total_upserts + total_excluidos

        logging.info(
            "Pipeline concluida | %s tabelas | enviados=%s | "
            "upserts=%s | excluidos=%s | sem alteracao=%s | "
            "afetados=%s | %.2f segundos",
            len(tabelas_hub),
            total_enviados,
            total_upserts,
            total_excluidos,
            total_ignorados,
            total_afetado,
            duracao_total,
        )

    finally:
        duck.close()


def executar() -> None:
    executar_pipeline()


if __name__ == "__main__":
    try:
        executar()
    except KeyboardInterrupt:
        logging.error("Execucao interrompida pelo usuario.")
        sys.exit(130)
    except Exception:
        logging.exception("Pipeline encerrada com erro.")
        sys.exit(1)