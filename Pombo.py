
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
import psycopg
from psycopg.rows import dict_row

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("pombo")

INTERVALO_LOOP = int(os.getenv("POMBO_INTERVALO_SEGUNDOS", "60"))
FETCH_SIZE = int(os.getenv("POMBO_FETCH_SIZE", "50000"))
IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Fonte:
    id: int
    nome: str
    id_referencia: int


@dataclass(frozen=True)
class ConfigODBC:
    ip: str
    driver: str
    base: str
    servidor: str
    usuario: str
    senha: str
    porta: int
    empresas: tuple[int, ...]
    nome_alias: str


@dataclass(frozen=True)
class Entidade:
    id: int
    id_fonte: int
    tabela: str
    tipo_carga: str
    intervalo_minutos: int
    tamanho_lote: int
    usar_trigger: bool
    ultima_execucao: datetime | None
    proxima_execucao: datetime | None
    carga_inicial_concluida: bool
    data_carga_inicial: datetime | None
    ultima_execucao_status: str | None


@dataclass(frozen=True)
class ConfigPombo:
    caminho: str
    tipo_destino: str
    usuario: str | None
    senha: str | None


def validar_identificador(nome: str) -> None:
    if not nome or not IDENTIFICADOR.fullmatch(nome):
        raise ValueError(f"Identificador SQL invalido: {nome!r}")


def normalizar_nome(valor: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", valor.strip().lower()).strip("_")


def hub_connect() -> psycopg.Connection:
    obrigatorias = {
        "HUB_CONTROLE_HOST": os.getenv("HUB_CONTROLE_HOST"),
        "HUB_CONTROLE_USER": os.getenv("HUB_CONTROLE_USER"),
        "HUB_CONTROLE_PASSWORD": os.getenv("HUB_CONTROLE_PASSWORD"),
    }
    faltantes = [k for k, v in obrigatorias.items() if not v]
    if faltantes:
        raise RuntimeError("Variaveis ausentes: " + ", ".join(faltantes))

    return psycopg.connect(
    host=obrigatorias["HUB_CONTROLE_HOST"],
    port=int(os.getenv("HUB_CONTROLE_PORT", "54320")),
    dbname=os.getenv("HUB_CONTROLE_DB", "central"),
    user=obrigatorias["HUB_CONTROLE_USER"],
    password=obrigatorias["HUB_CONTROLE_PASSWORD"],
    row_factory=dict_row,
)


def buscar_config_pombo(hub: psycopg.Connection) -> ConfigPombo:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT caminho, tipo_destino, usuario, senha
            FROM public.configuracao_pombo
            WHERE ativo = TRUE
            ORDER BY id
            LIMIT 1
        """)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Nenhuma configuracao do pombo ativa.")
    return ConfigPombo(
        caminho=row["caminho"],
        tipo_destino=(row.get("tipo_destino") or "SMB").upper(),
        usuario=row.get("usuario"),
        senha=row.get("senha"),
    )


def buscar_fontes(hub: psycopg.Connection) -> list[Fonte]:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT id, nome_fonte, id_referencia
            FROM public.fontes
            WHERE ativo = TRUE
              AND UPPER(tipo_fonte) = 'ODBC'
            ORDER BY ordem_execucao, id
        """)
        rows = cur.fetchall()
    return [Fonte(r["id"], r["nome_fonte"], r["id_referencia"]) for r in rows]


def buscar_config_odbc(hub: psycopg.Connection, id_referencia: int) -> ConfigODBC:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                ip_servidor,
                driver_odbc,
                nome_base,
                nome_server,
                usuario_db,
                senha_db,
                porta,
                cod_empresa,
                nome_alias
            FROM public.configuracao
            WHERE id = %s
            AND ativo = TRUE
        """, (id_referencia,))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Configuracao ODBC {id_referencia} nao encontrada.")

    empresas = tuple(
    int(x.strip())
    for x in (row.get("cod_empresa") or "").split(",")
    if x.strip()
        )
    return ConfigODBC(
        ip=row["ip_servidor"],
        driver=row["driver_odbc"],
        base=row["nome_base"],
        servidor=row["nome_server"],
        usuario=row["usuario_db"],
        senha=row["senha_db"],
        porta=int(row["porta"]),
        empresas=empresas,
        nome_alias=row["nome_alias"]
)


def buscar_entidades(hub: psycopg.Connection, id_fonte: int) -> list[Entidade]:
    with hub.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                id_fonte,
                nome_tabela,
                tipo_carga,
                intervalo_minutos,
                tamanho_lote,
                usar_trigger,
                ultima_execucao,
                proxima_execucao,
                carga_inicial_concluida,
                data_carga_inicial,
                ultima_execucao_status
            FROM public.configuracao_entidades
            WHERE id_fonte = %s
              AND ativo = TRUE
              AND (proxima_execucao IS NULL OR proxima_execucao <= CURRENT_TIMESTAMP)
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
            usar_trigger=bool(r.get("usar_trigger")),
            ultima_execucao=r.get("ultima_execucao"),
            proxima_execucao=r.get("proxima_execucao"),
            carga_inicial_concluida=bool(r.get("carga_inicial_concluida")),
            data_carga_inicial=r.get("data_carga_inicial"),
            ultima_execucao_status=r.get("ultima_execucao_status"),
        )
        for r in rows
    ]


def marcar_execucao(hub: psycopg.Connection, entidade: Entidade) -> None:
    with hub.cursor() as cur:
        cur.execute("""
            UPDATE public.configuracao_entidades
               SET ultima_execucao = CURRENT_TIMESTAMP,
                   proxima_execucao = CURRENT_TIMESTAMP + (%s * INTERVAL '1 minute')
             WHERE id = %s
        """, (entidade.intervalo_minutos, entidade.id))
    hub.commit()

def marcar_carga_inicial_concluida(hub, entidade):
    with hub.cursor() as cur:
        cur.execute(
            """
            UPDATE public.configuracao_entidades
            SET
                carga_inicial_concluida = TRUE,
                data_carga_inicial = CURRENT_TIMESTAMP,
                ultima_execucao_status = 'SUCESSO'
            WHERE id = %s
            """,
            (entidade.id,),
        )

    hub.commit()


def raiz_smb(caminho: str) -> str:
    partes = PureWindowsPath(caminho).parts
    if len(partes) < 2:
        raise ValueError(f"Caminho UNC invalido: {caminho}")
    return str(PureWindowsPath(partes[0], partes[1]))


def autenticar_smb(cfg: ConfigPombo) -> None:
    if cfg.tipo_destino != "SMB" or not cfg.usuario or not cfg.senha:
        return
    raiz = raiz_smb(cfg.caminho)
    result = subprocess.run(
        ["net", "use", raiz, cfg.senha, f"/user:{cfg.usuario}", "/persistent:no"],
        capture_output=True,
        text=True,
    )
    texto = (result.stdout + result.stderr).strip()
    if result.returncode != 0 and "1219" not in texto and "ja existe" not in texto.lower():
        raise RuntimeError(f"Falha no SMB {raiz}: {texto}")


def sybase_connect(cfg: ConfigODBC) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"UID={cfg.usuario};PWD={cfg.senha};"
        f"ENG={cfg.servidor};DBN={cfg.base};"
        f"LINKS=tcpip(host={cfg.ip};port={cfg.porta});"
    )
    return pyodbc.connect(conn_str, autocommit=False, timeout=30)


def obter_colunas(cursor: pyodbc.Cursor, tabela: str) -> list[str]:
    validar_identificador(tabela)
    cursor.execute(f'SELECT * FROM "{tabela}" WHERE 1 = 0')
    return [c[0] for c in cursor.description]


def tabela_tem_codempresa(cursor: pyodbc.Cursor, tabela: str) -> bool:
    return "codempresa" in {c.lower() for c in obter_colunas(cursor, tabela)}


def tabela_tem_codempresa_nulo(cursor: pyodbc.Cursor, tabela: str) -> bool:
    validar_identificador(tabela)
    cursor.execute(
        f'SELECT TOP 1 1 '
        f'FROM "{tabela}" '
        f'WHERE "CodEmpresa" IS NULL'
    )
    return cursor.fetchone() is not None


def montar_where(chaves: Sequence[tuple[str, Any]]) -> tuple[str, list[Any]]:
    partes, valores = [], []
    for coluna, valor in chaves:
        validar_identificador(coluna)
        partes.append(f'"{coluna}" = ?')
        try:
            valores.append(int(valor))
        except (ValueError, TypeError):
            valores.append(valor)
    if not partes:
        raise ValueError("Evento sem chaves validas.")
    return " AND ".join(partes), valores


def chaves_evento(row: Any, itens: bool) -> list[tuple[str, Any]]:
    # Estrutura real criada por voce:
    # ColunaChave1 -> CodEmpresa
    # ColunaChave2 -> Chave
    # ColunaChave3 -> Item
    chaves: list[tuple[str, Any]] = []
    if row.ColunaChave1 and row.CodEmpresa is not None:
        chaves.append((row.ColunaChave1.strip(), row.CodEmpresa))
    if row.ColunaChave2 and row.Chave is not None:
        chaves.append((row.ColunaChave2.strip(), row.Chave))
    if itens and row.ColunaChave3 and row.Item is not None:
        chaves.append((row.ColunaChave3.strip(), row.Item))
    return chaves


def row_dict(row: Any, colunas: Sequence[str]) -> dict[str, Any]:
    return dict(zip(colunas, row, strict=False))


def metadados(registro: dict[str, Any], fonte: Fonte, cfg_odbc: ConfigODBC, entidade: Entidade,
              acao: str, event_id: int | None, log_table: str | None,
              batch_id: str) -> dict[str, Any]:
    saida = dict(registro)
    saida.update({
        "_source_id": fonte.id,
        "_source_name": fonte.nome,
        "nome_alias": cfg_odbc.nome_alias,
        "_entity": entidade.tabela,
        "_operation": acao,
        "_event_id": event_id,
        "_event_table": log_table,
        "_batch_id": batch_id,
        "_ingestion_timestamp": datetime.now(timezone.utc),
    })
    return saida


def pasta_entidade(cfg_pombo: ConfigPombo, fonte: Fonte, entidade: Entidade) -> Path:
    return Path(cfg_pombo.caminho) / "Bronze" / normalizar_nome(fonte.nome_alias) / normalizar_nome(entidade.tabela)


def existe_parquet(pasta: Path) -> bool:
    return pasta.exists() and any(pasta.rglob("*.parquet"))


def gravar_parquet(registros: list[dict[str, Any]], pasta_base: Path, prefixo: str) -> Path:
    agora = datetime.now()
    pasta = pasta_base / f"ingestion_year={agora.year:04d}" / f"ingestion_month={agora.month:02d}" / f"ingestion_day={agora.day:02d}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"{prefixo}_{agora:%Y%m%d_%H%M%S_%f}_{uuid.uuid4().hex[:8]}.parquet"
    pq.write_table(pa.Table.from_pylist(registros), arquivo, compression="zstd", use_dictionary=True)
    return arquivo


def carga_inicial(conn: pyodbc.Connection, fonte: Fonte, cfg_odbc: ConfigODBC,
                  entidade: Entidade, pasta: Path) -> int:
    validar_identificador(entidade.tabela)
    cur = conn.cursor()
    sql = f'SELECT * FROM "{entidade.tabela}"'
    params: list[Any] = []

    if cfg_odbc.empresas and tabela_tem_codempresa(cur, entidade.tabela):
        if tabela_tem_codempresa_nulo(cur, entidade.tabela):
            log.info(
                "[%s/%s] CodEmpresa possui NULL; carga sem filtro de empresa",
                fonte.nome,
                entidade.tabela,
            )
        else:
            marks = ", ".join("?" for _ in cfg_odbc.empresas)
            sql += f' WHERE "CodEmpresa" IN ({marks})'
            params.extend(cfg_odbc.empresas)

            log.info(
                "[%s/%s] CodEmpresa obrigatorio; filtro aplicado: %s",
                fonte.nome,
                entidade.tabela,
                cfg_odbc.empresas,
            )

    log.info("[%s/%s] Carga inicial", fonte.nome, entidade.tabela)
    cur.execute(sql, params)
    colunas = [c[0] for c in cur.description]
    total, lote = 0, 0

    while True:
        rows = cur.fetchmany(FETCH_SIZE)
        if not rows:
            break
        lote += 1
        batch = f"FULL_{uuid.uuid4().hex}"
        dados = [metadados(row_dict(r, colunas), fonte, cfg_odbc, entidade, "SNAPSHOT", None, None, batch) for r in rows]
        arquivo = gravar_parquet(dados, pasta, f"full_{lote:06d}")
        total += len(dados)
        log.info("[%s/%s] %s linhas -> %s", fonte.nome, entidade.tabela, len(dados), arquivo)

    return total


def processar_log(conn: pyodbc.Connection, fonte: Fonte, cfg_odbc: ConfigODBC, entidade: Entidade,
                  pasta: Path, tabela_log: str, itens: bool) -> int:
    validar_identificador(tabela_log)
    validar_identificador(entidade.tabela)
    cur = conn.cursor()
    cur.execute(f'''
        SELECT TOP {entidade.tamanho_lote} *
          FROM "{tabela_log}"
         WHERE Sincronizado = 0
           AND NomeTabela = ?
         ORDER BY ID
    ''', (entidade.tabela,))
    eventos = cur.fetchall()
    if not eventos:
        return 0

    batch = f"INC_{uuid.uuid4().hex}"
    dados: list[dict[str, Any]] = []
    ids: list[int] = []

    for evento in eventos:
        try:
            chaves = chaves_evento(evento, itens)
            where_sql, valores = montar_where(chaves)
            acao = (evento.Acao or "UPDATE").strip().upper()

            if acao in {"INSERT", "UPDATE", "I", "U"}:
                cur.execute(f'SELECT * FROM "{entidade.tabela}" WHERE {where_sql}', valores)
                rows = cur.fetchall()
                colunas = [c[0] for c in cur.description]

                if rows:
                    for row in rows:
                        dados.append(metadados(row_dict(row, colunas), fonte, entidade, cfg_odbc, acao, evento.ID, tabela_log, batch))
                else:
                    # Sumiu antes da leitura: vira tombstone.
                    dados.append(metadados(dict(chaves), fonte, entidade, cfg_odbc, "DELETE", evento.ID, tabela_log, batch))

            elif acao in {"DELETE", "D"}:
                dados.append(metadados(dict(chaves), fonte, entidade, cfg_odbc, "DELETE", evento.ID, tabela_log, batch))
            else:
                log.warning("[%s] Acao desconhecida ID=%s: %s", tabela_log, evento.ID, acao)
                continue

            ids.append(int(evento.ID))
        except Exception:
            log.exception("[%s/%s] Erro no evento %s", tabela_log, entidade.tabela, evento.ID)

    if not dados:
        return 0

    arquivo = gravar_parquet(dados, pasta, normalizar_nome(tabela_log))
    marks = ", ".join("?" for _ in ids)
    cur.execute(f'UPDATE "{tabela_log}" SET Sincronizado = 1 WHERE ID IN ({marks})', ids)
    conn.commit()
    log.info("[%s/%s] %s eventos -> %s", fonte.nome, entidade.tabela, len(ids), arquivo)
    return len(ids)


def incremental(conn: pyodbc.Connection, fonte: Fonte, cfg_odbc:ConfigODBC, entidade: Entidade, pasta: Path) -> int:
    return (
        processar_log(conn, fonte, cfg_odbc, entidade, pasta, "z_datalake", False)
        + processar_log(conn, fonte, cfg_odbc, entidade, pasta, "z_datalake_itens", True)
    )


def processar_entidade(
    hub: psycopg.Connection,
    conn: pyodbc.Connection,
    fonte: Fonte,
    cfg_odbc: ConfigODBC,
    entidade: Entidade,
    cfg_pombo: ConfigPombo,
) -> None:
    pasta = pasta_entidade(cfg_pombo, cfg_odbc, entidade)

    if not entidade.carga_inicial_concluida:
        total = carga_inicial(conn, fonte, cfg_odbc, entidade, pasta)
        log.info("[%s/%s] Full concluido: %s linhas", fonte.nome, entidade.tabela, total)

    # Eventos acumulados durante o full são aplicados logo depois.
        if entidade.usar_trigger:
            incremental(conn, fonte, cfg_odbc, entidade, pasta)

        marcar_carga_inicial_concluida(hub, entidade)
    elif entidade.usar_trigger:
        total = incremental(conn, fonte, cfg_odbc, entidade, pasta)
        log.info("[%s/%s] Incremental: %s eventos", fonte.nome, entidade.tabela, total)
    else:
        log.info("[%s/%s] Snapshot ja existe; sem trigger nesta versao", fonte.nome, entidade.tabela)


def ciclo() -> None:
    with hub_connect() as hub:
        cfg_pombo = buscar_config_pombo(hub)
        autenticar_smb(cfg_pombo)

        for fonte in buscar_fontes(hub):
            try:
                cfg_odbc = buscar_config_odbc(hub, fonte.id_referencia)
                entidades = buscar_entidades(hub, fonte.id)
                if not entidades:
                    continue

                with sybase_connect(cfg_odbc) as sybase:
                    for entidade in entidades:
                        try:
                            processar_entidade(  hub,  sybase, fonte, cfg_odbc, entidade, cfg_pombo)
                            marcar_execucao(hub, entidade)
                        except Exception:
                            with hub.cursor() as cur:
                                cur.execute("""
                                    UPDATE public.configuracao_entidades
                                       SET ultima_execucao_status = 'ERRO'
                                     WHERE id = %s
                                """, (entidade.id,))
                            hub.commit()

                            log.exception(
                                "[%s/%s] Falha",
                                fonte.nome,
                                entidade.tabela,
                            )

            except Exception:
                log.exception("[%s] Falha na fonte", fonte.nome)


def main() -> int:
    once = "--once" in sys.argv
    log.info("Pombo Data Lake iniciado")
    while True:
        try:
            ciclo()
        except KeyboardInterrupt:
            return 0
        except Exception:
            log.exception("Erro geral no ciclo")

        if once:
            return 0
        time.sleep(INTERVALO_LOOP)


if __name__ == "__main__":
    raise SystemExit(main())