# pombo_refinador.py
#
# Arquitetura: a CONFIGURACAO DO CENTRAL define tudo.
#
#   tabela_hub (entidades ativas)
#       -> delimita os blocos do current_silver.sql
#   origens configuradas (fontes -> configuracao_api / configuracao)
#       -> define quais aliases fisicos do Bronze alimentam a entidade,
#          NA PRIMEIRA CARGA E NO INCREMENTAL
#   SELECAO de arquivos por alias(s) + janela mtime
#       ->
#   UMA execucao do bloco = UM resultado Current
#
# Nenhuma excecao por nome de tabela.
import os
import re
import time
import traceback
from datetime import datetime
from pathlib import Path

import duckdb
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()

POMBO_ID = int(os.getenv("POMBO_ID", "1"))

PASTA_SCRIPTS = Path(r"D:\Pipeline\Scripts")
PASTA_TEMP_DUCKDB = Path(r"D:\Pipeline\Temp_DuckDB")

SCRIPT_SILVER = PASTA_SCRIPTS / "current_silver.sql"
SCRIPT_GOLD = PASTA_SCRIPTS / "current_gold.sql"

SERVIDOR = os.getenv("POMBO_SERVIDOR_PIPELINE", r"\\10.68.152.218\d$")
BRONZE = Path(SERVIDOR) / "Pipeline" / "Bronze"
SILVER_CURRENT = Path(SERVIDOR) / "Pipeline" / "Silver" / "Current"
SILVER_TEMP = Path(SERVIDOR) / "Pipeline" / "Silver" / "Temp"


# ------------------------------------------------------------
# HUB / LOG
# ------------------------------------------------------------

def hub_connect() -> psycopg.Connection:
    obrigatorias = {
        "HUB_CONTROLE_HOST": os.getenv("HUB_CONTROLE_HOST"),
        "HUB_CONTROLE_USER": os.getenv("HUB_CONTROLE_USER"),
        "HUB_CONTROLE_PASSWORD": os.getenv("HUB_CONTROLE_PASSWORD"),
    }
    faltantes = [n for n, v in obrigatorias.items() if not v]
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


def inserir_log(nome_acao: str) -> int:
    with hub_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.logs_delta
                    (pombo_id, nome_acao, data_inicio, log)
                VALUES (%s, %s, NOW(), %s)
                RETURNING id;
                """,
                (POMBO_ID, nome_acao, f"Status: EM EXECUCAO\nEtapa: {nome_acao}"),
            )
            retorno = cur.fetchone()
            if retorno is None:
                raise RuntimeError("Nao foi possivel obter o ID do log.")
            conn.commit()
            return int(retorno["id"])


def finalizar_log(log_id, nome_acao, nome_script, inicio, sucesso, erro=None):
    duracao = time.monotonic() - inicio
    if sucesso:
        texto_log = (
            "Status: SUCESSO\n"
            f"Etapa: {nome_acao}\n"
            f"Script: {nome_script}\n"
            f"Tempo: {duracao:.2f} segundos"
        )
    else:
        texto_log = (
            "Status: ERRO\n"
            f"Etapa: {nome_acao}\n"
            f"Script: {nome_script}\n"
            f"Tempo: {duracao:.2f} segundos\n\n"
            f"Traceback:\n{erro or 'Erro sem traceback disponivel.'}"
        )
    with hub_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.logs_delta
                   SET data_fim = NOW(),
                       log = %s
                 WHERE id = %s;
                """,
                (texto_log, log_id),
            )
        conn.commit()


# ------------------------------------------------------------
# DUCKDB
# ------------------------------------------------------------

def sql_literal(valor: str) -> str:
    return valor.replace("'", "''")


def anexar_central(conexao):
    host = os.getenv("HUB_CONTROLE_HOST")
    user = os.getenv("HUB_CONTROLE_USER")
    password = os.getenv("HUB_CONTROLE_PASSWORD")
    port = os.getenv("HUB_CONTROLE_PORT", "54320")
    dbname = os.getenv("HUB_CONTROLE_DB", "central")
    if not host or not user or not password:
        raise RuntimeError("Variaveis ausentes: HUB_CONTROLE_HOST/USER/PASSWORD")
    conexao.execute("INSTALL postgres;")
    conexao.execute("LOAD postgres;")
    dsn = (
        f"host={sql_literal(host)} port={int(port)} "
        f"dbname={sql_literal(dbname)} user={sql_literal(user)} "
        f"password={sql_literal(password)}"
    )
    conexao.execute(f"ATTACH '{dsn}' AS central (TYPE POSTGRES);")


def criar_conexao_duckdb():
    PASTA_TEMP_DUCKDB.mkdir(parents=True, exist_ok=True)
    conexao = duckdb.connect()
    conexao.execute(f"SET temp_directory = '{PASTA_TEMP_DUCKDB.as_posix()}';")
    conexao.execute(f"SET memory_limit = '{os.getenv('DUCKDB_MEMORY_LIMIT', '32GB')}';")
    conexao.execute(f"SET threads = {int(os.getenv('DUCKDB_THREADS', '4'))};")
    conexao.execute("SET preserve_insertion_order = false;")
    anexar_central(conexao)
    return conexao


def ler_sql(arquivo: Path) -> str:
    if not arquivo.is_file():
        raise FileNotFoundError(f"Script SQL nao encontrado: {arquivo}")
    sql = arquivo.read_text(encoding="utf-8")
    if not sql.strip():
        raise ValueError(f"O script {arquivo.name} esta vazio.")
    return sql


# ------------------------------------------------------------
# SELECAO FISICA DE ARQUIVOS BRONZE
#
# aliases=None significa "todas as origens" e so deve acontecer
# quando o Central NAO tem origem configurada para a entidade
# (fallback defensivo). Com origens configuradas, apenas os
# aliases cadastrados entram na leitura - na primeira carga e
# no incremental.
# ------------------------------------------------------------

def _arquivos_bronze_novos(entidade_bronze, current: Path, overlap_minutos: int, aliases=None):
    """Retorna (candidatos_na_janela, doador_de_schema).

    O doador e o arquivo mais recente da entidade (fora ou dentro da janela).
    Ele e anexado a leitura do delta para ancora de schema: arquivos
    harmonizados/parciais podem nao conter todas as colunas que o bloco
    referencia (ex.: Municipio so existe no Sybase). union_by_name com o
    doador garante o schema completo; as linhas dele sao descartadas pelo
    checkpoint _ingestion_timestamp > MAX(Current)."""
    if aliases:
        padroes = [f"{a}/{entidade_bronze}/**/*.parquet" for a in aliases]
    else:
        padroes = [f"*/{entidade_bronze}/**/*.parquet"]

    limite = current.stat().st_mtime - overlap_minutos * 60
    candidatos = []
    doadores = {}  # alias.lower() -> (path, mtime); um por origem configurada
    for padrao in padroes:
        for p in BRONZE.glob(padrao):
            try:
                if not (p.is_file() and p.suffix.lower() == ".parquet"):
                    continue
                mt = p.stat().st_mtime
            except OSError:
                continue
            alias = p.relative_to(BRONZE).parts[0].lower()
            d = doadores.get(alias)
            if d is None or mt > d[1]:
                doadores[alias] = (p, mt)
            if mt >= limite:
                candidatos.append(p)

    vistos = set()
    unicos = []
    for p in candidatos:
        chave = str(p).lower()
        if chave not in vistos:
            vistos.add(chave)
            unicos.append(p)
    return sorted(unicos, key=lambda x: x.stat().st_mtime), [d[0] for d in doadores.values()]


def _arquivos_bronze_todos(entidade_bronze, aliases=None):
    """Todos os arquivos Bronze da entidade, SEM janela mtime.

    Usado no bootstrap automatico: o incremental seleciona por janela,
    mas na primeira carga tudo que existe e dado."""
    if aliases:
        padroes = [f"{a}/{entidade_bronze}/**/*.parquet" for a in aliases]
    else:
        padroes = [f"*/{entidade_bronze}/**/*.parquet"]
    vistos = set()
    arquivos = []
    for padrao in padroes:
        for p in BRONZE.glob(padrao):
            try:
                if not (p.is_file() and p.suffix.lower() == ".parquet"):
                    continue
            except OSError:
                continue
            chave = str(p).lower()
            if chave not in vistos:
                vistos.add(chave)
                arquivos.append(p)
    return sorted(arquivos, key=lambda x: x.stat().st_mtime)


def _bootstrap_current_auto(conexao, nome_tabela: str, bloco_sql: str, aliases) -> bool:
    """Bootstrap AUTOMATICO de bloco com checkpoint interno.

    1) lista TODO o Bronze da entidade (sem janela);
    2) cria o Current vazio com o schema bruto do Bronze;
    3) migra para o schema snake do bloco via _garantir_current_compativel;
    4) executa o proprio bloco sobre todos os arquivos (o bloco precisa
       suportar checkpoint NULL: 'WHERE c.ultima_ingestao IS NULL OR ...').

    Sem arquivos: nao cria nada e retorna False; o proximo ciclo tenta
    de novo. Nunca exige intervenção manual."""
    current = SILVER_CURRENT / f"{nome_tabela}.parquet"
    leitura, padrao = _extrair_leitura_bronze(bloco_sql)
    if leitura is None:
        print(f"[INCREMENTAL] {nome_tabela}: bootstrap automatico: leitura Bronze nao reconhecida.")
        return False
    entidade_bronze = leitura.group("entidade")
    arquivos = _arquivos_bronze_todos(entidade_bronze, aliases or None)
    if not arquivos:
        print(f"[INCREMENTAL] {nome_tabela}: bootstrap automatico sem arquivos Bronze; aguardando dados.")
        return False

    lista = _lista_duckdb_arquivos(arquivos)
    conexao.execute(
        f"COPY (SELECT * FROM read_parquet({lista}, union_by_name=true) WHERE FALSE) "
        f"TO '{current.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    _garantir_current_compativel(conexao, nome_tabela, bloco_sql, current)

    bloco_exec, qtd = padrao.subn(
        f"read_parquet({lista}, union_by_name=true)", bloco_sql, count=1
    )
    if qtd != 1:
        current.unlink(missing_ok=True)
        raise RuntimeError(
            f"{nome_tabela}: bootstrap automatico nao conseguiu restringir a leitura Bronze."
        )
    tmp = SILVER_CURRENT / f"tmp_{nome_tabela}_incremental.parquet"
    if tmp.exists():
        tmp.unlink()
    conexao.execute(bloco_exec)
    n = conexao.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}', union_by_name=true)"
    ).fetchone()[0]
    if n == 0:
        tmp.unlink(missing_ok=True)
        print(
            f"[INCREMENTAL] {nome_tabela}: bootstrap gerou 0 linhas. "
            "O bloco SQL suporta checkpoint NULL "
            "('WHERE c.ultima_ingestao IS NULL OR ...')?"
        )
        return True
    promover_tmp(nome_tabela, exigir_tmp=True)
    print(
        f"[INCREMENTAL] {nome_tabela}: bootstrap automatico concluido "
        f"({len(arquivos)} arquivo(s), {n} linha(s))."
    )
    return True


def _lista_duckdb_arquivos(arquivos) -> str:
    return "[" + ",".join("'" + a.as_posix().replace("'", "''") + "'" for a in arquivos) + "]"


def promover_tmp(nome_tabela: str, exigir_tmp: bool = False):
    """Promove TMP. Na primeira carga o SQL pode gravar direto no Current.

    Nos caminhos incrementais use exigir_tmp=True: se o bloco nao gerou
    TMP, algo saiu errado e o Current deve ser preservado.
    """
    tmp = SILVER_CURRENT / f"tmp_{nome_tabela}_incremental.parquet"
    atual = SILVER_CURRENT / f"{nome_tabela}.parquet"

    if tmp.is_file() and tmp.stat().st_size > 0:
        os.replace(tmp, atual)
        return

    if exigir_tmp:
        raise RuntimeError(f"{nome_tabela}: TMP ausente/vazio; Current preservado.")

    if atual.is_file() and atual.stat().st_size > 0:
        return

    raise RuntimeError(f"{nome_tabela}: TMP ausente/vazio e Current nao foi gerado.")


# ------------------------------------------------------------
# INTROSPECCAO DOS BLOCOS SQL
# ------------------------------------------------------------

def _normalizar_destino(nome: str) -> str:
    nome = nome.strip()
    if nome.startswith("tmp_") and nome.endswith("_incremental"):
        nome = nome[4:-12]
    return nome.lower()


def _checkpoint_current(conexao, current: Path):
    caminho = current.as_posix().replace("'", "''")
    retorno = conexao.execute(
        f"SELECT MAX(_ingestion_timestamp) "
        f"FROM read_parquet('{caminho}', union_by_name=true)"
    ).fetchone()
    return retorno[0] if retorno else None


def _extrair_leitura_bronze(bloco_sql: str):
    padrao = re.compile(
        r"""read_parquet\s*\(\s*['"]
            (?P<caminho>(?:(?:\\\\)|//)[^'"]*?[/\\]+Bronze[/\\]+
            (?P<alias>[^/\\'"]+)[/\\]+(?P<entidade>[^/\\'"]+)[/\\]+\*\*[/\\]+\*\.parquet)
            ['"]\s*,\s*union_by_name\s*=\s*true\s*\)""",
        flags=re.IGNORECASE | re.VERBOSE,
    )
    return padrao.search(bloco_sql), padrao


def _restringir_bronze_a_aliases(bloco_sql: str, aliases):
    """Primeira carga: substitui o glob Bronze do bloco por um glob por
    alias configurado no Central. aliases=None mantem o comportamento
    original (todas as origens fisicas)."""
    if not aliases:
        return bloco_sql
    leitura, padrao = _extrair_leitura_bronze(bloco_sql)
    if leitura is None:
        return bloco_sql
    caminho = leitura.group("caminho")
    alias = leitura.group("alias")
    prefixo, _, sufixo = caminho.partition(alias)
    gabs = [prefixo + a + sufixo for a in aliases]
    lista = "[" + ",".join("'" + g.replace("'", "''") + "'" for g in gabs) + "]"
    bloco, qtd = padrao.subn(
        f"read_parquet({lista}, union_by_name=true)", bloco_sql, count=1
    )
    if qtd != 1:
        raise RuntimeError(
            f"primeira carga: nao foi possivel restringir o Bronze aos aliases {aliases}."
        )
    return bloco


def _bloco_le_current(bloco_sql: str, nome_tabela: str) -> bool:
    """True se o bloco LE o Current (checkpoint interno proprio)."""
    padrao = re.compile(
        r"(?is)read_parquet\s*\(\s*['\"][^'\"]*[\\/]Silver[\\/]Current[\\/]"
        + re.escape(nome_tabela)
        + r"\.parquet['\"]"
    )
    return padrao.search(bloco_sql) is not None


def _extrair_chaves_partition(bloco_sql: str):
    m = re.search(
        r"(?is)ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+(.*?)\s+ORDER\s+BY",
        bloco_sql,
    )
    if not m:
        return []
    return [x.strip() for x in m.group(1).split(",") if x.strip()]


def _mapa_saida_silver(bloco_sql: str):
    matches = list(re.finditer(
        r"(?is)\bSELECT\s+(.*?)\s+FROM\s+origem_rankeada\s+WHERE\s+_rn\s*=\s*1",
        bloco_sql,
    ))
    if not matches:
        return {}
    select_final = matches[0].group(1)
    mapa = {}
    for origem, destino in re.findall(
        r"(?im)^\s*[\"']?([A-Za-z_][A-Za-z0-9_]*)[\"']?\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*,?\s*$",
        select_final,
    ):
        mapa[origem.lower()] = destino
    for linha in select_final.splitlines():
        linha = linha.strip().rstrip(",")
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", linha):
            mapa.setdefault(linha.lower(), linha)
    return mapa


SQL_PALAVRAS = {
    "cast", "as", "varchar", "bigint", "integer", "int", "text", "double",
    "timestamp", "date", "boolean", "null", "select", "from", "where",
    "limit", "true", "false",
}


def _extrair_saidas_novos(bloco_sql: str) -> dict:
    """alias snake -> ("COL"|"EXPR"|"NULL", fonte/expressao), extraido do
    proprio bloco anti-join (novos_raw + novos). Sem regra de negocio."""
    i0 = bloco_sql.index("novos_raw AS (")
    i1 = bloco_sql.index("atual_sem_afetados AS (")
    trecho = bloco_sql[i0:i1]
    saidas = {}
    for linha in trecho.splitlines():
        linha = linha.strip().rstrip(",")
        if not linha or linha.startswith(
            ("--", "WITH", "FROM", "WHERE", "CROSS", "PARTITION",
             "ORDER", "SELECT", ")", "*")
        ):
            continue
        m = re.match(r"^(.*?)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)$", linha)
        if m:
            expr, alias = m.group(1).strip(), m.group(2)
            if alias == "_rn" or "ROW_NUMBER" in expr:
                continue
            if expr.upper().startswith("NULL::"):
                saidas[alias] = ("NULL", expr.split("::", 1)[1].strip())
            else:
                saidas[alias] = ("EXPR", expr)
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", linha):
            saidas.setdefault(linha, ("COL", linha))
    return saidas


def _resolver_expr(expr, saidas, cols_atual, profundidade=0):
    refs = {r.lower() for r in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)} - SQL_PALAVRAS
    falt = [r for r in refs if r not in cols_atual]
    if not falt or profundidade > 3:
        return expr, falt
    e2 = expr
    for r in falt:
        for a2, (t2, src2) in saidas.items():
            if a2.lower() == r:
                e2 = re.sub(rf"\b{re.escape(a2)}\b", f"({src2})", e2)
                break
        else:
            return e2, [r]
    return _resolver_expr(e2, saidas, cols_atual, profundidade + 1)


def _tipo_coluna_bronze(conexao, bloco_sql: str, coluna: str):
    """Tipo de dados da coluna no schema unido do Bronze (glob do bloco)."""
    leitura, _ = _extrair_leitura_bronze(bloco_sql)
    if leitura is None:
        return None
    caminho = leitura.group("caminho").replace("'", "''")
    col = coluna.replace('"', '""')
    try:
        rows = conexao.execute(
            f"SELECT data_type FROM (DESCRIBE SELECT \"{col}\" "
            f"FROM read_parquet('{caminho}', union_by_name=true))"
        ).fetchall()
        return rows[0][0] if rows else None
    except Exception:
        return None


def _garantir_current_compativel(conexao, nome_tabela: str, bloco_sql: str, current: Path) -> None:
    """Blocos anti-join (novos_raw) exigem Current snake. Se o Current
    estiver num schema antigo (ex.: camelCase legado), migra automaticamente
    usando o mapeamento fonte->alias do proprio bloco: generico, sem nome
    de tabela. Backup com timestamp + validacao de row count."""
    if "novos_raw AS (" not in bloco_sql or "atual_sem_afetados AS (" not in bloco_sql:
        return
    saidas = _extrair_saidas_novos(bloco_sql)
    if not saidas:
        return

    rows = conexao.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{current.as_posix()}', union_by_name=true)"
    ).fetchall()
    cols_atual = {r[0].lower() for r in rows}

    faltantes = [a for a in saidas if a.lower() not in cols_atual]
    if not faltantes:
        return

    partes, novas = [], []
    for alias, (tipo, expr) in saidas.items():
        al = alias.lower()
        if al in cols_atual:
            partes.append(f'"{alias}"')
        else:
            e2, falt = (None, []) if tipo == "NULL" else _resolver_expr(expr, saidas, cols_atual)
            if e2 is not None and not falt:
                partes.append(f"{e2} AS \"{alias}\"")
            else:
                # Coluna NOVA (nenhuma fonte equivalente no Current): o proprio bloco
                # SQL a introduz via UNION ALL BY NAME a partir do delta do Bronze.
                novas.append(alias)

    if novas:
        # Garante a coluna no Current AGORA (NULL tipado). Nao podemos depender
        # do delta para introduzi-la: com 0 candidatos o bloco nao executa e a
        # Gold quebraria. Valores reais chegam quando o delta existir.
        detalhes = []
        for a in novas:
            tipo0, expr0 = saidas[a]
            if tipo0 == "NULL":
                partes.append(f"CAST(NULL AS {expr0}) AS \"{a}\"")
                detalhes.append(f"{a}::{expr0}")
            else:
                fonte = expr0 if tipo0 == "COL" else None
                t = _tipo_coluna_bronze(conexao, bloco_sql, fonte or a) if fonte else None
                partes.append(f"CAST(NULL AS {t or 'VARCHAR'}) AS \"{a}\"")
                detalhes.append(f"{a}::{t or 'VARCHAR'}")
        print(
            f"[MIGRACAO] {nome_tabela}: coluna(s) nova(s) ausente(s) no Current; "
            f"adicionada(s) como NULL tipado ({', '.join(detalhes)}); "
            f"valores reais chegam via delta do Bronze."
        )

    # Reescrita so quando o schema efetivamente muda (renome ou coluna nova).
    if not any(" AS " in p for p in partes):
        print(f"[MIGRACAO] {nome_tabela}: schema ja compativel; reescrita do Current desnecessaria.")
        return

    tmp = current.with_name(f"tmp_migracao_{current.stem}.parquet")
    if tmp.exists():
        tmp.unlink()
    conexao.execute(
        f"COPY (SELECT {', '.join(partes)} "
        f"FROM read_parquet('{current.as_posix()}', union_by_name=true)) "
        f"TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    n_orig = conexao.execute(
        f"SELECT COUNT(*) FROM read_parquet('{current.as_posix()}', union_by_name=true)"
    ).fetchone()[0]
    n_novo = conexao.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}', union_by_name=true)"
    ).fetchone()[0]
    if n_orig != n_novo:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"{nome_tabela}: migracao alterou row count ({n_orig} -> {n_novo}); "
            "abortada, Current original preservado."
        )

    backup = current.with_name(
        f"{current.stem}_pre_snake_{datetime.now():%Y%m%d_%H%M%S}.parquet"
    )
    os.replace(current, backup)
    os.replace(tmp, current)
    print(
        f"[MIGRACAO] {nome_tabela}: Current migrado para snake "
        f"({len(faltantes)} colunas, {n_novo} linhas); backup={backup.name}"
    )


def _executar_update_central_do_bloco(conexao, bloco_sql: str):
    updates = re.findall(
        r"(?is)(UPDATE\s+central\.public\.configuracao_entidades.*?;)",
        bloco_sql,
    )
    for update in updates:
        conexao.execute(update)


# ------------------------------------------------------------
# CONFIG DO CENTRAL
# ------------------------------------------------------------

def _tabelas_hub_configuradas(conexao):
    """Entidades ativas com tabela_hub. Sem filtro de tipo_carga:
    snapshot continua passando pelo Refinador (passada classica;
    na maioria dos runs a janela mtime vem vazia)."""
    sql = """
        SELECT DISTINCT TRIM(tabela_hub) AS tabela_hub
        FROM central.public.configuracao_entidades
        WHERE ativo IS TRUE
          AND tabela_hub IS NOT NULL
          AND TRIM(tabela_hub) <> ''
        ORDER BY TRIM(tabela_hub)
    """
    rows = conexao.execute(sql).fetchall()
    return {str(row[0]).strip().lower() for row in rows if row[0]}


def _origens_configuradas_entidade(conexao, nome_tabela: str):
    """Aliases fisicos que alimentam a entidade, segundo o Central.

    Nao filtra tipo_carga: a SELECAO de arquivos deve respeitar todas
    as origens cadastradas (incrementais e snapshots), na primeira carga
    e no incremental. O roteamento decide o motor pela estrutura do bloco."""
    sql = """
        SELECT ce.id AS id_entidade, f.id AS id_fonte,
               UPPER(TRIM(f.tipo_fonte)) AS tipo_fonte,
               CASE
                 WHEN UPPER(TRIM(f.tipo_fonte))='API' THEN NULLIF(TRIM(ca.nome_alias),'')
                 WHEN UPPER(TRIM(f.tipo_fonte))='ODBC' AND UPPER(TRIM(c.engine))='SYBASE'
                     THEN NULLIF(TRIM(c.nome_alias),'')
                 ELSE NULL
               END AS alias_fisico
        FROM central.public.configuracao_entidades ce
        JOIN central.public.fontes f ON f.id = ce.id_fonte
        LEFT JOIN central.public.configuracao_api ca
               ON UPPER(TRIM(f.tipo_fonte))='API' AND ca.id = f.id_referencia
        LEFT JOIN central.public.configuracao c
               ON UPPER(TRIM(f.tipo_fonte))='ODBC' AND c.id = f.id_referencia AND c.ativo IS TRUE
        WHERE ce.ativo IS TRUE
          AND ce.tabela_hub = ?
    """
    rows = conexao.execute(sql, [nome_tabela]).fetchall()
    origens = []
    for id_entidade, id_fonte, tipo_fonte, alias_fisico in rows:
        tipo = str(tipo_fonte or "").strip().upper()
        alias = str(alias_fisico or "").strip()
        if tipo == "API":
            if not alias:
                raise RuntimeError(
                    f"{nome_tabela}: fonte API {id_fonte} sem nome_alias em configuracao_api."
                )
            origens.append({
                "tipo": "API", "alias_fisico": alias,
                "is_postos": alias.upper() == "POSTOS",
                "id_entidade": id_entidade, "id_fonte": id_fonte,
            })
        elif tipo == "ODBC" and alias:
            origens.append({
                "tipo": "SYBASE", "alias_fisico": alias,
                "is_postos": False,
                "id_entidade": id_entidade, "id_fonte": id_fonte,
            })
    return origens


# ------------------------------------------------------------
# PARSER DO current_silver.sql
# Delimitador: somente os destinos Silver/Current (cruzados com
# tabela_hub). Silver/Temp nao define entidade.
# ------------------------------------------------------------

def separar_tabelas_silver(sql: str, tabelas_hub):
    destinos = list(re.finditer(
        r"(?is)\bTO\s+[\"'](?P<caminho>[^\"']*/Silver/Current/(?P<tabela>[^/\"']+)\.parquet)[\"']",
        sql,
    ))
    if not destinos:
        raise RuntimeError("Nenhum COPY para Silver/Current encontrado no current_silver.sql.")
    if tabelas_hub is None:
        raise RuntimeError("A lista de tabela_hub do Central e obrigatoria.")

    tabelas_hub = {str(x).strip().lower() for x in tabelas_hub if str(x).strip()}
    destinos = [
        d for d in destinos
        if _normalizar_destino(d.group("tabela")) in tabelas_hub
    ]
    if not destinos:
        raise RuntimeError("Nenhum destino Silver/Current corresponde a tabela_hub.")

    blocos = []
    inicio_regiao = 0
    for indice, destino in enumerate(destinos):
        nome_tabela = _normalizar_destino(destino.group("tabela"))

        regiao_antes = sql[inicio_regiao:destino.start()]
        copies_antes = list(re.finditer(r"\bCOPY\s*\(", regiao_antes, flags=re.IGNORECASE))
        inicio_bloco = inicio_regiao + copies_antes[0].start() if copies_antes else inicio_regiao

        if indice + 1 < len(destinos):
            proximo_to = destinos[indice + 1]
            regiao_depois = sql[destino.end():proximo_to.start()]
            copies_depois = list(re.finditer(r"\bCOPY\s*\(", regiao_depois, flags=re.IGNORECASE))
            fim_bloco = destino.end() + copies_depois[0].start() if copies_depois else proximo_to.start()
        else:
            fim_bloco = len(sql)

        bloco = sql[inicio_bloco:fim_bloco].strip()
        bloco = re.sub(r"(?im)^\s*DETACH\s+central\s*;\s*$", "", bloco).strip()

        blocos.append((nome_tabela, bloco))
        inicio_regiao = destino.end()
    return blocos


# ------------------------------------------------------------
# MOTOR POSTOS (blocos que LEEM o Current, origem POSTOS pura)
# ------------------------------------------------------------

def executar_incremental_postos_fisico(conexao, nome_tabela: str, bloco_sql: str, alias_fisico: str):
    nome = nome_tabela
    atual = SILVER_CURRENT / f"{nome}.parquet"

    if not _bloco_le_current(bloco_sql, nome):
        raise RuntimeError(
            f"{nome}: motor POSTOS exige bloco que LE o Current. "
            "Bloco classico/bootstrap deve usar o caminho delta+merge."
        )

    alias_fisico = str(alias_fisico or "").strip()
    if not alias_fisico:
        raise RuntimeError(f"{nome}: alias fisico nao resolvido no Central; execucao cancelada.")

    if not atual.is_file():
        print(f"[INCREMENTAL] {nome}: Current inexistente -> primeira carga (alias={alias_fisico}).")
        conexao.execute(_restringir_bronze_a_aliases(bloco_sql, [alias_fisico]))
        promover_tmp(nome)
        return

    leitura, padrao_leitura = _extrair_leitura_bronze(bloco_sql)
    if leitura is None:
        raise RuntimeError(
            f"{nome}: leitura Bronze nao reconhecida; Current preservado."
        )

    entidade_bronze = leitura.group("entidade")
    overlap = int(os.getenv("SILVER_BRONZE_OVERLAP_MINUTOS", "10"))
    arquivos, doadores = _arquivos_bronze_novos(entidade_bronze, atual, overlap, aliases=[alias_fisico])

    print(
        f"[INCREMENTAL] {nome}: aliases=[{alias_fisico}]; "
        f"{len(arquivos)} arquivo(s) Bronze candidato(s) + doador schema, overlap={overlap} min."
    )
    if not arquivos:
        print(f"[INCREMENTAL] {nome}: sem arquivos novos; Current mantido.")
        return

    extras = [d for d in doadores if d not in arquivos]
    if extras:
        arquivos = arquivos + extras
    lista = _lista_duckdb_arquivos(arquivos)
    bloco_otimizado, qtd_bronze = padrao_leitura.subn(
        f"read_parquet({lista}, union_by_name=true)", bloco_sql, count=1
    )
    if qtd_bronze != 1:
        raise RuntimeError(
            f"{nome}: leitura Bronze nao localizada exatamente uma vez. "
            "Execucao cancelada para proteger o Current."
        )

    tmp = SILVER_CURRENT / f"tmp_{nome}_incremental.parquet"
    if tmp.exists():
        tmp.unlink()

    # Se o bloco gravar direto no Current, redireciona o unico destino para TMP.
    if not re.search(r"(?is)" + re.escape(f"tmp_{nome}_incremental.parquet"), bloco_otimizado):
        destino_current_re = re.compile(
            rf"(?is)\bTO\s+(['\"])[^'\"]*[\\/]{re.escape(nome)}\.parquet\1"
        )
        bloco_otimizado, qtd_dest = destino_current_re.subn(
            f"TO '{tmp.as_posix().replace(chr(39), chr(39) * 2)}'",
            bloco_otimizado,
            count=1,
        )
        if qtd_dest != 1:
            raise RuntimeError(
                f"{nome}: destino Current nao localizado para redirecionar; "
                "execucao cancelada para proteger o Current."
            )

    conexao.execute(bloco_otimizado)
    promover_tmp(nome, exigir_tmp=True)
    print(f"[INCREMENTAL] {nome}: Current atualizado.")


# ------------------------------------------------------------
# CAMINHO PADRAO: delta + merge (blocos classicos)
# ------------------------------------------------------------

def executar_incremental_padrao(
    conexao,
    nome_tabela: str,
    bloco_sql: str,
    aliases=None,
    aplicar_checkpoint_ingestao: bool = True,
):
    current = SILVER_CURRENT / f"{nome_tabela}.parquet"

    if not current.is_file():
        # Primeira carga tambem respeita os aliases cadastrados no Central.
        rotulo = ",".join(aliases) if aliases else "*"
        print(f"[INCREMENTAL] {nome_tabela}: Current inexistente -> primeira carga (aliases=[{rotulo}]).")
        conexao.execute(_restringir_bronze_a_aliases(bloco_sql, aliases))
        promover_tmp(nome_tabela)
        return

    leitura, padrao_leitura = _extrair_leitura_bronze(bloco_sql)
    if leitura is None:
        print(f"[INCREMENTAL] {nome_tabela}: leitura Bronze nao reconhecida; usando bloco original.")
        conexao.execute(bloco_sql)
        promover_tmp(nome_tabela)
        return

    entidade_bronze = leitura.group("entidade")
    overlap = int(os.getenv("SILVER_BRONZE_OVERLAP_MINUTOS", "10"))
    arquivos, doadores = _arquivos_bronze_novos(entidade_bronze, current, overlap, aliases=aliases)

    rotulo_aliases = ",".join(aliases) if aliases else "*"
    print(
        f"[INCREMENTAL] {nome_tabela}: aliases=[{rotulo_aliases}]; "
        f"{len(arquivos)} arquivo(s) Bronze candidato(s) + {len(doadores)} doador(es) schema, overlap={overlap} min."
    )
    if not arquivos:
        print(f"[INCREMENTAL] {nome_tabela}: sem arquivos novos; Current mantido.")
        return

    extras = [d for d in doadores if d not in arquivos]
    if extras:
        arquivos = arquivos + extras
    lista = _lista_duckdb_arquivos(arquivos)

    if aplicar_checkpoint_ingestao:
        checkpoint = _checkpoint_current(conexao, current)
        if checkpoint is None:
            print(f"[INCREMENTAL] {nome_tabela}: Current sem checkpoint; usando bloco original.")
            conexao.execute(bloco_sql)
            promover_tmp(nome_tabela)
            return
        checkpoint_sql = str(checkpoint).replace("'", "''")
        leitura_delta = (
            "(SELECT * FROM read_parquet(" + lista + ", union_by_name=true) "
            "WHERE _ingestion_timestamp > TIMESTAMP '" + checkpoint_sql + "')"
        )
    else:
        leitura_delta = "read_parquet(" + lista + ", union_by_name=true)"

    bloco_delta, qtd = padrao_leitura.subn(leitura_delta, bloco_sql, count=1)
    if qtd != 1:
        raise RuntimeError(f"{nome_tabela}: nao foi possivel trocar a leitura Bronze.")

    # Bloco com checkpoint proprio: apenas reduz fisicamente o Bronze.
    if _bloco_le_current(bloco_sql, nome_tabela):
        print(f"[INCREMENTAL] {nome_tabela}: bloco ja incremental; Bronze fisico reduzido.")
        conexao.execute(bloco_delta)
        promover_tmp(nome_tabela, exigir_tmp=True)
        return

    # Bloco classico: transforma so o delta e mescla com o Current.
    delta = SILVER_TEMP / f"delta_{nome_tabela}.parquet"
    tmp = SILVER_CURRENT / f"tmp_{nome_tabela}_incremental.parquet"
    SILVER_TEMP.mkdir(parents=True, exist_ok=True)
    for arquivo in (delta, tmp):
        if arquivo.exists():
            arquivo.unlink()

    destino_current_re = re.compile(
        rf"(?is)\bTO\s+(['\"])[^'\"]*[\\/]{re.escape(nome_tabela)}\.parquet\1"
    )
    destino_delta = delta.as_posix().replace("'", "''")
    bloco_delta, qtd_dest = destino_current_re.subn(f"TO '{destino_delta}'", bloco_delta, count=1)
    if qtd_dest != 1:
        raise RuntimeError(f"{nome_tabela}: destino Current nao localizado exatamente uma vez.")

    conexao.execute(bloco_delta)

    if not delta.is_file() or delta.stat().st_size == 0:
        print(f"[INCREMENTAL] {nome_tabela}: nenhum registro novo apos checkpoint.")
        if delta.exists():
            delta.unlink()
        return

    chaves_bronze = _extrair_chaves_partition(bloco_sql)
    mapa = _mapa_saida_silver(bloco_sql)

    if chaves_bronze:
        chaves_silver = []
        for chave in chaves_bronze:
            destino = mapa.get(chave.lower())
            if not destino:
                raise RuntimeError(
                    f"{nome_tabela}: nao consegui mapear a chave Bronze '{chave}' "
                    f"para a coluna Silver. Current preservado."
                )
            chaves_silver.append(destino)

        partition = ", ".join(f'"{c}"' for c in chaves_silver)
        anti_join = " AND ".join(
            f'd."{c}" IS NOT DISTINCT FROM a."{c}"' for c in chaves_silver
        )
        atual_sql = current.as_posix().replace("'", "''")
        delta_sql = delta.as_posix().replace("'", "''")
        tmp_sql = tmp.as_posix().replace("'", "''")

        # Dedup apenas do delta (rank pequeno) + anti-join por hash no Current.
        # Sem ordenar Current ∪ delta inteiro.
        merge_sql = f"""
        COPY (
            WITH delta_dedup AS (
                SELECT * EXCLUDE (_dedup_rn)
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition}
                               ORDER BY _ingestion_timestamp DESC, _event_id DESC
                           ) AS _dedup_rn
                    FROM read_parquet('{delta_sql}', union_by_name=true)
                )
                WHERE _dedup_rn = 1
            ),
            atual_sem_afetados AS (
                SELECT a.*
                FROM read_parquet('{atual_sql}', union_by_name=true) a
                WHERE NOT EXISTS (
                    SELECT 1 FROM delta_dedup d WHERE {anti_join}
                )
            )
            SELECT * FROM atual_sem_afetados
            UNION ALL BY NAME
            SELECT * FROM delta_dedup
        )
        TO '{tmp_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    else:
        atual_sql = current.as_posix().replace("'", "''")
        delta_sql = delta.as_posix().replace("'", "''")
        tmp_sql = tmp.as_posix().replace("'", "''")
        merge_sql = f"""
        COPY (
            SELECT *
            FROM read_parquet(['{atual_sql}', '{delta_sql}'], union_by_name=true)
        )
        TO '{tmp_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """

    conexao.execute(merge_sql)
    promover_tmp(nome_tabela, exigir_tmp=True)
    delta.unlink(missing_ok=True)
    _executar_update_central_do_bloco(conexao, bloco_sql)
    print(f"[INCREMENTAL] {nome_tabela}: Current atualizado.")


# ------------------------------------------------------------
# ROTEADOR
#
# UMA entidade = UMA execucao do bloco = UM resultado Current.
# As origens do Central servem para SELECIONAR os arquivos Bronze
# (na primeira carga e no incremental); nunca para executar o
# bloco mais de uma vez.
# ------------------------------------------------------------

def executar_incremental_por_origem(conexao, nome_tabela: str, bloco_sql: str):
    current = SILVER_CURRENT / f"{nome_tabela}.parquet"
    origens = _origens_configuradas_entidade(conexao, nome_tabela)
    aliases = list(dict.fromkeys(o["alias_fisico"] for o in origens if o.get("alias_fisico")))

    if not current.is_file():
        if _bloco_le_current(bloco_sql, nome_tabela):
            # Bootstrap AUTOMATICO: o proprio Refinador constroi o Current a
            # partir de todo o Bronze da entidade e ingere no mesmo ciclo.
            # Sem dados ainda: avisa e segue, SEM derrubar a etapa.
            _bootstrap_current_auto(conexao, nome_tabela, bloco_sql, aliases)
            return
        # Primeira carga tambem respeita as origens cadastradas no Central.
        if not aliases:
            print(
                f"[INCREMENTAL] {nome_tabela}: sem origens configuradas; "
                "primeira carga usando todas as origens fisicas (fallback)."
            )
        executar_incremental_padrao(conexao, nome_tabela, bloco_sql, aliases=aliases or None)
        return

    if not origens:
        print(
            f"[INCREMENTAL] {nome_tabela}: sem origens configuradas no Central; "
            "usando todas as origens fisicas (fallback)."
        )
        executar_incremental_padrao(
            conexao, nome_tabela, bloco_sql,
            aliases=None,
            aplicar_checkpoint_ingestao=True,
        )
        return

    postos = [o for o in origens if o["is_postos"]]
    aliases_postos = list(
        dict.fromkeys(o["alias_fisico"] for o in postos)
    )
    if len(aliases_postos) > 1:
        raise RuntimeError(
            f"{nome_tabela}: mais de um alias fisico POSTOS-distinto "
            f"no Central: {aliases_postos}."
        )
    demais = [o for o in origens if not o["is_postos"]]

    if _bloco_le_current(bloco_sql, nome_tabela):
        # Bloco com checkpoint interno proprio (le o Current).
        # Uma unica execucao; a selecao fisica cobre todas as origens.
        # Migra Current com schema antigo (ex.: camel legado) se preciso.
        _garantir_current_compativel(conexao, nome_tabela, bloco_sql, current)
        if postos and not demais:
            executar_incremental_postos_fisico(
                conexao, nome_tabela, bloco_sql,
                alias_fisico=postos[0]["alias_fisico"],
            )
            return
        executar_incremental_padrao(
            conexao, nome_tabela, bloco_sql,
            aliases=aliases,
            aplicar_checkpoint_ingestao=False,
        )
        return

    # Bloco classico (so le Bronze): uma unica passada delta+merge.
    #  - exclusiva Sybase: somente os aliases cadastrados (POSTOS nunca entra);
    #  - exclusiva POSTOS bootstrap (itens): delta via buckets + merge;
    #  - hibrida harmonizada: POSTOS + Sybase no mesmo delta e merge.
    executar_incremental_padrao(
        conexao, nome_tabela, bloco_sql,
        aliases=aliases,
        aplicar_checkpoint_ingestao=True,
    )


# ------------------------------------------------------------
# EXECUCAO
# ------------------------------------------------------------

def executar_silver_tabela_por_tabela(conexao, arquivo):
    sql = ler_sql(arquivo)
    tabelas_hub = _tabelas_hub_configuradas(conexao)
    print(f"Central: {len(tabelas_hub)} tabela_hub configurada(s).")

    blocos = separar_tabelas_silver(sql, tabelas_hub)
    total = len(blocos)
    print(f"Silver: {total} tabelas encontradas.")
    print("Tabelas Silver:")
    for numero in range(total, 0, -1):
        nome, _ = blocos[numero - 1]
        print(f"  {numero:02d}. {nome}")

    abortar_no_erro = os.getenv("SILVER_ABORTAR_NO_ERRO", "0") == "1"
    falhas = []
    for indice in range(total - 1, -1, -1):
        numero = indice + 1
        nome, bloco = blocos[indice]
        inicio = time.monotonic()
        print(f"[SILVER {numero}/{total}] {nome}...")
        try:
            executar_incremental_por_origem(conexao, nome, bloco)
        except Exception:
            duracao = time.monotonic() - inicio
            print(f"[SILVER ERRO] {nome} apos {duracao:.2f}s")
            traceback.print_exc()
            falhas.append(nome)
            if abortar_no_erro:
                raise
            continue
        duracao = time.monotonic() - inicio
        print(f"[SILVER OK] {nome} - {duracao:.2f}s")

    if falhas:
        print(
            f"[SILVER] ciclo concluido com {len(falhas)} falha(s): "
            + ", ".join(falhas)
            + ". As demais tabelas foram processadas; as falhas serao "
            "tentadas novamente no proximo ciclo."
        )


def executar_sql(conexao, arquivo):
    conexao.execute(ler_sql(arquivo))


def executar_etapa(conexao_duck, nome_acao, arquivo, tabela_por_tabela=False):
    print(f"Executando {arquivo.name}...")
    inicio = time.monotonic()
    log_id = inserir_log(nome_acao)
    try:
        if tabela_por_tabela:
            executar_silver_tabela_por_tabela(conexao_duck, arquivo)
        else:
            executar_sql(conexao_duck, arquivo)
        finalizar_log(log_id, nome_acao, arquivo.name, inicio, True)
        print(f"{arquivo.name}: OK")
    except Exception:
        erro_original = traceback.format_exc()
        try:
            finalizar_log(log_id, nome_acao, arquivo.name, inicio, False, erro_original)
        except Exception:
            print("Falha adicional ao gravar o erro no PostgreSQL:")
            traceback.print_exc()
        print(f"Falha na etapa {nome_acao}. As etapas seguintes nao serao executadas.")
        raise


def main():
    print("========== POMBO REFINADOR ==========")
    for arq in (SCRIPT_SILVER, SCRIPT_GOLD):
        if not arq.is_file():
            raise FileNotFoundError(f"Script SQL nao encontrado: {arq}")

    duck = criar_conexao_duckdb()
    try:
        executar_etapa(duck, "Silver", SCRIPT_SILVER, tabela_por_tabela=True)
        executar_etapa(duck, "Gold", SCRIPT_GOLD, tabela_por_tabela=False)
        print("Refinamento concluido: Silver Current e Gold Current geradas.")
    finally:
        duck.close()


def executar():
    main()


if __name__ == "__main__":
    executar()