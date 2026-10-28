# pombo_replica_homolog.py
#
# Replica 1x ao dia o schema gold do hub_dw para o hub_dw_homolog:
# UPSERT por tabela, usando como chave a unique index ux_*_pombo ja
# existente no destino. Espelho fiel: registros que sairam da origem
# sao removidos do destino (anti-join pela temp).
#
# Nenhuma tabela ou chave hardcoded: tudo vem do catalogo.
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

SCHEMA = "gold"
TAMANHO_LOTE = 10_000


# Bootstrap: primeira carga. TRUNCA a tabela de destino e insere puro
# (sem ON CONFLICT / anti-join) - muito mais rapido que upsert sobre base
# populada. Rode UMA vez via:
#   python replicador_dw_homolog.py --bootstrap
# (ou HUB_BOOTSTRAP=1). As execucoes 2x/dia normais rodam SEM bootstrap
# (upsert + anti-join de orfaos).
MODO_BOOTSTRAP = (
    os.getenv("HUB_BOOTSTRAP", "0") == "1" or "--bootstrap" in sys.argv
)

PASTA_LOG = Path(r"C:\Replicador\Log")


def configurar_logging() -> None:
    PASTA_LOG.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                PASTA_LOG / f"replica_homolog_{datetime.now():%Y%m%d}.log",
                encoding="utf-8",
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )


def carregar_configuracoes() -> tuple[dict, dict]:
    host = os.getenv("HUB_CONTROLE_HOST", "").strip()
    porta_texto = os.getenv("HUB_CONTROLE_PORT", "54320").strip()
    user = os.getenv("HUB_DW_USER", "").strip()
    password = os.getenv("HUB_DW_PASSWORD", "")

    faltantes = [
        n
        for n, v in {
            "HUB_CONTROLE_HOST": host,
            "HUB_DW_USER": user,
            "HUB_DW_PASSWORD": password,
        }.items()
        if not v
    ]
    if faltantes:
        raise RuntimeError("Variaveis ausentes: " + ", ".join(faltantes))

    try:
        porta = int(porta_texto)
    except ValueError as exc:
        raise RuntimeError(
            "HUB_CONTROLE_PORT deve ser um numero inteiro."
        ) from exc

    comum = dict(
        host=host, port=porta, user=user, password=password,
        connect_timeout=15, row_factory=dict_row,
    )
    origem = dict(comum, dbname=os.getenv("HUB_DW_DB", "hub_dw"))
    destino = dict(
        comum,
        dbname=os.getenv("HUB_DW_HOMOLOG_DB", "hub_dw_homolog"),
    )
    return origem, destino


def conectar(cfg: dict, application_name: str) -> psycopg.Connection:
    return psycopg.connect(application_name=application_name, **cfg)


def listar_tabelas_gold(conn: psycopg.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
            """,
            (SCHEMA,),
        )
        return [str(r["table_name"]) for r in cur.fetchall()]


def listar_colunas(conn: psycopg.Connection, tabela: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (SCHEMA, tabela),
        )
        return [str(r["column_name"]) for r in cur.fetchall()]


def obter_chave_unica(
    conn: psycopg.Connection, tabela: str
) -> tuple[str, list[str]] | None:
    """Primeira unique index (preferindo ux_*_pombo) da tabela no destino."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ic.relname AS index_name,
                   array_agg(a.attname ORDER BY ord.n) AS colunas
            FROM pg_index i
            JOIN pg_class t   ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_class ic  ON ic.oid = i.indexrelid
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS ord(attnum, n)
            JOIN pg_attribute a
              ON a.attrelid = t.oid AND a.attnum = ord.attnum
            WHERE n.nspname = %s
              AND t.relname = %s
              AND i.indisunique
              AND i.indpred IS NULL
              AND i.indexprs IS NULL
              AND a.attnum > 0
            GROUP BY ic.relname
            ORDER BY
                CASE
                    WHEN left(ic.relname, 3) = 'ux_'
                     AND right(ic.relname, 6) = '_pombo'
                    THEN 0
                    ELSE 1
                END,
                ic.relname
            LIMIT 1;
            """,
            (SCHEMA, tabela),
        )
        linha = cur.fetchone()
    if linha is None:
        return None
    return str(linha["index_name"]), [str(c) for c in linha["colunas"]]


def _q(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def replicar_tabela(
    origem: psycopg.Connection,
    destino: psycopg.Connection,
    tabela: str,
    colunas: list[str],
    chaves: list[str],
) -> tuple[int, int, int]:
    """Copia a tabela da origem para a temp do destino e aplica upsert
    + remocao de orfaos. A copia usa tubo COPY binario direto entre os
    dois Postgres (sem materializar linhas em Python); se o psycopg da
    venv nao suportar copy.read(), cai no modo loteado com fetchmany.
    """
    nome_tmp = f"tmp_rep_{tabela}"
    colunas_str = ", ".join(_q(c) for c in colunas)

    criar_tmp = sql.SQL(
        "CREATE TEMP TABLE {} "
        "(LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP"
    ).format(
        sql.Identifier(nome_tmp),
        sql.Identifier(SCHEMA),
        sql.Identifier(tabela),
    )

    copy_out_sql = (
        f"COPY (SELECT {colunas_str} "
        f"FROM {_q(SCHEMA)}.{_q(tabela)}) TO STDOUT WITH (FORMAT binary)"
    )
    copy_in_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT binary)"
    ).format(
        sql.Identifier(nome_tmp),
        sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
    )

    atribuicoes = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(c), sql.Identifier(c)
        )
        for c in colunas
        if c not in chaves
    )

    if atribuicoes:
        condicao_alteracao = sql.SQL(" OR ").join(
            sql.SQL("destino.{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(c), sql.Identifier(c)
            )
            for c in colunas
            if c not in chaves
        )
        upsert = sql.SQL(
            "INSERT INTO {}.{} AS destino ({}) "
            "SELECT {} FROM {} "
            "ON CONFLICT ({}) DO UPDATE SET {} WHERE {}"
        ).format(
            sql.Identifier(SCHEMA), sql.Identifier(tabela),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.Identifier(nome_tmp),
            sql.SQL(", ").join(sql.Identifier(c) for c in chaves),
            atribuicoes, condicao_alteracao,
        )
    else:
        upsert = sql.SQL(
            "INSERT INTO {}.{} ({}) "
            "SELECT {} FROM {} "
            "ON CONFLICT ({}) DO NOTHING"
        ).format(
            sql.Identifier(SCHEMA), sql.Identifier(tabela),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.Identifier(nome_tmp),
            sql.SQL(", ").join(sql.Identifier(c) for c in chaves),
        )

    anti_join = sql.SQL(" AND ").join(
        sql.SQL("dest.{} IS NOT DISTINCT FROM tmp.{}").format(
            sql.Identifier(c), sql.Identifier(c)
        )
        for c in chaves
    )
    remover_orfaos = sql.SQL(
        "DELETE FROM {}.{} AS dest "
        "WHERE NOT EXISTS (SELECT 1 FROM {} AS tmp WHERE {})"
    ).format(
        sql.Identifier(SCHEMA), sql.Identifier(tabela),
        sql.Identifier(nome_tmp), anti_join,
    )

    select_origem = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
        sql.Identifier(SCHEMA), sql.Identifier(tabela),
    )
    copy_tmp_loteado = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(nome_tmp),
        sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
    )

    if MODO_BOOTSTRAP:
        truncar = sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(SCHEMA), sql.Identifier(tabela)
        )
        inserir_puro = sql.SQL(
            "INSERT INTO {}.{} ({}) SELECT {} FROM {}"
        ).format(
            sql.Identifier(SCHEMA), sql.Identifier(tabela),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
            sql.Identifier(nome_tmp),
        )

    enviados = 0
    with origem.transaction():
        with destino.transaction():
            with destino.cursor() as cur:
                if MODO_BOOTSTRAP:
                    cur.execute(truncar)
                cur.execute(criar_tmp)

            bytes_copiados = 0
            proximo_log = 256 * 1024 * 1024
            with origem.cursor() as cur_leitura:
                with cur_leitura.copy(copy_out_sql) as copia_out:
                    tubo = hasattr(copia_out, "read")
                    if tubo:
                        with destino.cursor() as cur_escrita:
                            with cur_escrita.copy(copy_in_sql) as copia_in:
                                while True:
                                    chunk = copia_out.read()
                                    if not chunk:
                                        break
                                    copia_in.write(chunk)
                                    bytes_copiados += len(chunk)
                                    if bytes_copiados >= proximo_log:
                                        logging.info(
                                            "  %s.%s | ~%.0f MB copiados...",
                                            SCHEMA, tabela,
                                            bytes_copiados / 1048576,
                                        )
                                        proximo_log += 256 * 1024 * 1024
                    else:
                        # psycopg antigo: modo loteado (mais lento)
                        cur_leitura.execute(select_origem)
                        while True:
                            lote = cur_leitura.fetchmany(TAMANHO_LOTE)
                            if not lote:
                                break
                            with destino.cursor() as cur_escrita:
                                with cur_escrita.copy(copy_tmp_loteado) as copia:
                                    for linha in lote:
                                        copia.write_row(
                                            tuple(linha[c] for c in colunas)
                                        )
                            enviados += len(lote)
                            if enviados % (TAMANHO_LOTE * 50) < TAMANHO_LOTE:
                                logging.info(
                                    "  %s.%s | %s linhas copiadas...",
                                    SCHEMA, tabela, enviados,
                                )

            with destino.cursor() as cur:
                if tubo:
                    cur.execute(
                        sql.SQL("SELECT COUNT(*) AS n FROM {}").format(
                            sql.Identifier(nome_tmp)
                        )
                    )
                    enviados = int(cur.fetchone()["n"])
                if MODO_BOOTSTRAP:
                    logging.info(
                        "  %s.%s | cópia concluída (%s linhas); insert puro (bootstrap)...",
                        SCHEMA, tabela, enviados,
                    )
                    cur.execute(inserir_puro)
                    upserts = max(cur.rowcount, 0)
                    excluidos = 0
                else:
                    logging.info(
                        "  %s.%s | cópia concluída (%s linhas); aplicando upsert...",
                        SCHEMA, tabela, enviados,
                    )
                    cur.execute(upsert)
                    upserts = max(cur.rowcount, 0)
                    logging.info(
                        "  %s.%s | upsert aplicado (%s); removendo órfãos...",
                        SCHEMA, tabela, upserts,
                    )
                    cur.execute(remover_orfaos)
                    excluidos = max(cur.rowcount, 0)

    return enviados, upserts, excluidos

def executar() -> int:
    configurar_logging()
    logging.info("Replicacao hub_dw -> hub_dw_homolog iniciada.")
    if MODO_BOOTSTRAP:
        logging.info("MODO BOOTSTRAP: destino sera TRUNCADO antes da carga.")

    cfg_origem, cfg_destino = carregar_configuracoes()

    total_upserts = 0
    total_excluidos = 0
    total_enviados = 0
    falhas: list[str] = []

    with conectar(cfg_origem, "Pombo_Replica_Homolog_Origem") as origem, conectar(
        cfg_destino, "Pombo_Replica_Homolog_Destino"
    ) as destino:
        tabelas = listar_tabelas_gold(origem)
        logging.info("%s tabela(s) em %s.%s.", len(tabelas), cfg_origem["dbname"], SCHEMA)

        total_tabelas = len(tabelas)
        for posicao, tabela in enumerate(tabelas, start=1):
            inicio = time.perf_counter()
            logging.info(
                "Replicando %s.%s (%s/%s)...",
                SCHEMA, tabela, posicao, total_tabelas,
            )
            try:
                colunas_destino = listar_colunas(destino, tabela)
                if not colunas_destino:
                    raise RuntimeError(
                        f"tabela ausente no destino ({cfg_destino['dbname']}.{SCHEMA}.{tabela})"
                    )

                colunas_origem = listar_colunas(origem, tabela)
                colunas = [c for c in colunas_origem if c in set(colunas_destino)]
                if not colunas:
                    raise RuntimeError("nenhuma coluna em comum entre origem e destino")

                chave = obter_chave_unica(destino, tabela)
                if chave is None:
                    raise RuntimeError(
                        "nenhuma unique index (ux_*_pombo ou outra) no destino"
                    )
                nome_indice, colunas_chave = chave

                enviados, upserts, excluidos = replicar_tabela(
                    origem, destino, tabela, colunas, colunas_chave
                )

                duracao = time.perf_counter() - inicio
                total_enviados += enviados
                total_upserts += upserts
                total_excluidos += excluidos
                logging.info(
                    "OK | %s.%s | chave=%s(%s) | enviados=%s | upserts=%s | "
                    "excluidos=%s | %.2f segundos",
                    SCHEMA, tabela, nome_indice, ",".join(colunas_chave),
                    enviados, upserts, excluidos, duracao,
                )
            except Exception:
                falhas.append(tabela)
                logging.exception("FALHA | %s.%s | replicacao abortada para a tabela.", SCHEMA, tabela)

    logging.info(
        "Replicacao concluida | enviados=%s | upserts=%s | excluidos=%s | falhas=%s",
        total_enviados, total_upserts, total_excluidos,
        ", ".join(falhas) if falhas else "nenhuma",
    )
    return 1 if falhas else 0


if __name__ == "__main__":
    try:
        sys.exit(executar())
    except KeyboardInterrupt:
        logging.error("Execucao interrompida pelo usuario.")
        sys.exit(130)
    except Exception:
        logging.exception("Replicacao encerrada com erro.")
        sys.exit(1)