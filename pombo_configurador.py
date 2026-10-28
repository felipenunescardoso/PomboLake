"""
Pombo_Configurador.py

CRUD administrativo do banco central do Pombo.

Hierarquia:
    configuracao_pombo
        ├── configuracao              # configuração relacional / SGBD
        │       └── fontes
        ├── configuracao_api
        │       └── fontes
        └── configuracao_planilhas
                └── fontes

    fontes
        └── configuracao_entidades

Observação:
    A tabela relacional continua se chamando public.configuracao no schema.

Dependência:
    pip install psycopg[binary]
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterable, Literal

import psycopg
from psycopg.rows import dict_row


# =============================================================================
# CONFIGURAÇÃO DO POSTGRESQL
# Mesmo formato usado no extrator: valores definidos no próprio código.
# Depois você pode substituir por .env.
# =============================================================================

HUB_CONTROLE_HOST = "localhost"
HUB_CONTROLE_PORT = 54320
HUB_CONTROLE_DB = "central"
HUB_CONTROLE_USER = "central_pombo"
HUB_CONTROLE_PASSWORD = "Refit@2026"


TipoFonte = Literal["ODBC", "API", "PLANILHA"]

TABELA_CONFIGURACAO_POR_TIPO: dict[str, str] = {
    "ODBC": "configuracao",
    "API": "configuracao_api",
    "PLANILHA": "configuracao_planilhas",
}


# =============================================================================
# CONEXÃO
# =============================================================================

def hub_connect() -> psycopg.Connection:
    """Abre conexão com o banco central do Pombo."""
    return psycopg.connect(
        host=HUB_CONTROLE_HOST,
        port=HUB_CONTROLE_PORT,
        dbname=HUB_CONTROLE_DB,
        user=HUB_CONTROLE_USER,
        password=HUB_CONTROLE_PASSWORD,
        row_factory=dict_row,
        connect_timeout=10,
    )


@contextmanager
def transacao():
    """Executa uma operação com commit ou rollback automático."""
    conn = hub_connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _buscar_um(
    conn: psycopg.Connection,
    sql: str,
    parametros: tuple[Any, ...],
    mensagem: str,
) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(sql, parametros)
        registro = cursor.fetchone()

    if registro is None:
        raise ValueError(mensagem)

    return registro


def _validar_id(
    conn: psycopg.Connection,
    tabela: str,
    registro_id: int,
    mensagem: str,
) -> None:
    tabelas_permitidas = {
        "configuracao_pombo",
        "configuracao",
        "configuracao_api",
        "configuracao_planilhas",
        "fontes",
        "configuracao_entidades",
    }

    if tabela not in tabelas_permitidas:
        raise ValueError("Tabela não permitida para validação.")

    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT 1 FROM public.{tabela} WHERE id = %s",
            (registro_id,),
        )
        existe = cursor.fetchone()

    if existe is None:
        raise ValueError(mensagem)


# =============================================================================
# CONFIGURACAO_POMBO — AMBIENTES
# =============================================================================

def criar_ambiente(
    descricao: str,
    tipo_destino: str,
    caminho: str,
    usuario: str | None = None,
    senha: str | None = None,
    ativo: bool = True,
) -> dict[str, Any]:
    with transacao() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.configuracao_pombo
                (
                    descricao,
                    tipo_destino,
                    caminho,
                    usuario,
                    senha,
                    ativo
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (descricao, tipo_destino, caminho, usuario, senha, ativo),
            )
            return cursor.fetchone()


def listar_ambientes(ativo: bool | None = None) -> list[dict[str, Any]]:
    sql = "SELECT * FROM public.configuracao_pombo"
    parametros: tuple[Any, ...] = ()

    if ativo is not None:
        sql += " WHERE ativo = %s"
        parametros = (ativo,)

    sql += " ORDER BY id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, parametros)
            return cursor.fetchall()


def buscar_ambiente(ambiente_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.configuracao_pombo WHERE id = %s",
            (ambiente_id,),
            "Ambiente Pombo não encontrado.",
        )


def alterar_ambiente(
    ambiente_id: int,
    descricao: str,
    tipo_destino: str,
    caminho: str,
    usuario: str | None,
    senha: str | None,
    ativo: bool,
) -> dict[str, Any]:
    with transacao() as conn:
        return _buscar_um(
            conn,
            """
            UPDATE public.configuracao_pombo
               SET descricao = %s,
                   tipo_destino = %s,
                   caminho = %s,
                   usuario = %s,
                   senha = %s,
                   ativo = %s,
                   data_alteracao = CURRENT_TIMESTAMP
             WHERE id = %s
         RETURNING *
            """,
            (descricao, tipo_destino, caminho, usuario, senha, ativo, ambiente_id),
            "Ambiente Pombo não encontrado.",
        )


def desativar_ambiente(ambiente_id: int) -> dict[str, Any]:
    with transacao() as conn:
        return _buscar_um(
            conn,
            """
            UPDATE public.configuracao_pombo
               SET ativo = FALSE,
                   data_alteracao = CURRENT_TIMESTAMP
             WHERE id = %s
         RETURNING *
            """,
            (ambiente_id,),
            "Ambiente Pombo não encontrado.",
        )


# =============================================================================
# CONFIGURACAO — RELACIONAL / SGBD / ODBC
# =============================================================================

def criar_configuracao_relacional(
    id_configuracao_inicial: int,
    ip_servidor: str | None,
    driver_odbc: str | None,
    nome_base: str | None,
    nome_server: str | None,
    usuario_db: str | None,
    senha_db: str | None,
    porta: int | None,
    cod_empresa: str | None,
    engine: str | None,
    nome_alias: str | None,
    ativo: bool = True,
) -> dict[str, Any]:
    with transacao() as conn:
        _validar_id(
            conn,
            "configuracao_pombo",
            id_configuracao_inicial,
            "Ambiente Pombo não encontrado.",
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.configuracao
                (
                    ip_servidor,
                    driver_odbc,
                    nome_base,
                    nome_server,
                    usuario_db,
                    senha_db,
                    porta,
                    cod_empresa,
                    ativo,
                    engine,
                    nome_alias,
                    id_configuracao_inicial
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                RETURNING *
                """,
                (
                    ip_servidor,
                    driver_odbc,
                    nome_base,
                    nome_server,
                    usuario_db,
                    senha_db,
                    porta,
                    cod_empresa,
                    ativo,
                    engine,
                    nome_alias,
                    id_configuracao_inicial,
                ),
            )
            return cursor.fetchone()


def listar_configuracoes_relacionais(
    id_configuracao_inicial: int | None = None,
    ativo: bool | None = None,
) -> list[dict[str, Any]]:
    filtros: list[str] = []
    parametros: list[Any] = []

    if id_configuracao_inicial is not None:
        filtros.append("id_configuracao_inicial = %s")
        parametros.append(id_configuracao_inicial)

    if ativo is not None:
        filtros.append("ativo = %s")
        parametros.append(ativo)

    sql = "SELECT * FROM public.configuracao"
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(parametros))
            return cursor.fetchall()


def buscar_configuracao_relacional(configuracao_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.configuracao WHERE id = %s",
            (configuracao_id,),
            "Configuração relacional não encontrada.",
        )


def alterar_configuracao_relacional(
    configuracao_id: int,
    ip_servidor: str | None,
    driver_odbc: str | None,
    nome_base: str | None,
    nome_server: str | None,
    usuario_db: str | None,
    senha_db: str | None,
    porta: int | None,
    cod_empresa: str | None,
    engine: str | None,
    nome_alias: str | None,
    ativo: bool,
) -> dict[str, Any]:
    with transacao() as conn:
        return _buscar_um(
            conn,
            """
            UPDATE public.configuracao
               SET ip_servidor = %s,
                   driver_odbc = %s,
                   nome_base = %s,
                   nome_server = %s,
                   usuario_db = %s,
                   senha_db = %s,
                   porta = %s,
                   cod_empresa = %s,
                   engine = %s,
                   nome_alias = %s,
                   ativo = %s
             WHERE id = %s
         RETURNING *
            """,
            (
                ip_servidor,
                driver_odbc,
                nome_base,
                nome_server,
                usuario_db,
                senha_db,
                porta,
                cod_empresa,
                engine,
                nome_alias,
                ativo,
                configuracao_id,
            ),
            "Configuração relacional não encontrada.",
        )


def desativar_configuracao_relacional(configuracao_id: int) -> dict[str, Any]:
    with transacao() as conn:
        return _buscar_um(
            conn,
            """
            UPDATE public.configuracao
               SET ativo = FALSE
             WHERE id = %s
         RETURNING *
            """,
            (configuracao_id,),
            "Configuração relacional não encontrada.",
        )


# =============================================================================
# CONFIGURACAO_API
# =============================================================================

def criar_configuracao_api(
    id_configuracao_inicial: int,
    nome_api: str,
    descricao: str | None,
    metodo_http: str,
    endpoint_base: str,
    rota_recurso: str | None = None,
    tipo_autenticacao: str | None = None,
    usuario_api: str | None = None,
    senha_api: str | None = None,
    token_api: str | None = None,
    api_key: str | None = None,
    api_key_header: str | None = None,
    headers_json: Any = None,
    parametros_json: Any = None,
    timeout_segundos: int = 30,
    qtd_tentativas: int = 3,
    intervalo_retry_seg: int = 10,
    paginacao_ativa: bool = False,
    tipo_paginacao: str | None = None,
    parametro_pagina: str | None = None,
    parametro_limite: str | None = None,
    tamanho_pagina: int | None = None,
    campo_dados: str | None = None,
    campo_proxima_pagina: str | None = None,
    ativo: bool = True,
) -> dict[str, Any]:
    with transacao() as conn:
        _validar_id(
            conn,
            "configuracao_pombo",
            id_configuracao_inicial,
            "Ambiente Pombo não encontrado.",
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.configuracao_api
                (
                    nome_api,
                    descricao,
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
                    ativo,
                    id_configuracao_inicial
                )
                VALUES
                (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                RETURNING *
                """,
                (
                    nome_api,
                    descricao,
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
                    ativo,
                    id_configuracao_inicial,
                ),
            )
            return cursor.fetchone()


def listar_configuracoes_api(
    id_configuracao_inicial: int | None = None,
    ativo: bool | None = None,
) -> list[dict[str, Any]]:
    filtros: list[str] = []
    parametros: list[Any] = []

    if id_configuracao_inicial is not None:
        filtros.append("id_configuracao_inicial = %s")
        parametros.append(id_configuracao_inicial)

    if ativo is not None:
        filtros.append("ativo = %s")
        parametros.append(ativo)

    sql = "SELECT * FROM public.configuracao_api"
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(parametros))
            return cursor.fetchall()


def buscar_configuracao_api(configuracao_api_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.configuracao_api WHERE id = %s",
            (configuracao_api_id,),
            "Configuração de API não encontrada.",
        )


def alterar_configuracao_api(
    configuracao_api_id: int,
    **campos: Any,
) -> dict[str, Any]:
    campos_permitidos = {
        "nome_api",
        "descricao",
        "metodo_http",
        "endpoint_base",
        "rota_recurso",
        "tipo_autenticacao",
        "usuario_api",
        "senha_api",
        "token_api",
        "api_key",
        "api_key_header",
        "headers_json",
        "parametros_json",
        "timeout_segundos",
        "qtd_tentativas",
        "intervalo_retry_seg",
        "paginacao_ativa",
        "tipo_paginacao",
        "parametro_pagina",
        "parametro_limite",
        "tamanho_pagina",
        "campo_dados",
        "campo_proxima_pagina",
        "ativo",
        "id_configuracao_inicial",
    }
    return _alterar_por_campos(
        tabela="configuracao_api",
        registro_id=configuracao_api_id,
        campos=campos,
        campos_permitidos=campos_permitidos,
        possui_data_alteracao=True,
        mensagem="Configuração de API não encontrada.",
    )


def desativar_configuracao_api(configuracao_api_id: int) -> dict[str, Any]:
    return alterar_configuracao_api(configuracao_api_id, ativo=False)


# =============================================================================
# CONFIGURACAO_PLANILHAS
# =============================================================================

def criar_configuracao_planilha(
    id_configuracao_inicial: int,
    nome_configuracao: str,
    descricao: str | None,
    caminho_arquivo: str,
    nome_arquivo: str,
    abas: str,
    usuario_acesso: str | None = None,
    senha_acesso: str | None = None,
    tipo_arquivo: str = "XLSX",
    possui_cabecalho: bool = True,
    linha_cabecalho: int = 1,
    intervalo_leitura: str | None = None,
    coluna_data_referencia: str | None = None,
    destino_entidade: str | None = None,
    modo_carga: str = "INCREMENTAL",
    ativo: bool = True,
) -> dict[str, Any]:
    with transacao() as conn:
        _validar_id(
            conn,
            "configuracao_pombo",
            id_configuracao_inicial,
            "Ambiente Pombo não encontrado.",
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.configuracao_planilhas
                (
                    nome_configuracao,
                    descricao,
                    caminho_arquivo,
                    nome_arquivo,
                    abas,
                    usuario_acesso,
                    senha_acesso,
                    tipo_arquivo,
                    possui_cabecalho,
                    linha_cabecalho,
                    intervalo_leitura,
                    coluna_data_referencia,
                    destino_entidade,
                    modo_carga,
                    ativo,
                    id_configuracao_inicial
                )
                VALUES
                (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                RETURNING *
                """,
                (
                    nome_configuracao,
                    descricao,
                    caminho_arquivo,
                    nome_arquivo,
                    abas,
                    usuario_acesso,
                    senha_acesso,
                    tipo_arquivo,
                    possui_cabecalho,
                    linha_cabecalho,
                    intervalo_leitura,
                    coluna_data_referencia,
                    destino_entidade,
                    modo_carga,
                    ativo,
                    id_configuracao_inicial,
                ),
            )
            return cursor.fetchone()


def listar_configuracoes_planilhas(
    id_configuracao_inicial: int | None = None,
    ativo: bool | None = None,
) -> list[dict[str, Any]]:
    filtros: list[str] = []
    parametros: list[Any] = []

    if id_configuracao_inicial is not None:
        filtros.append("id_configuracao_inicial = %s")
        parametros.append(id_configuracao_inicial)

    if ativo is not None:
        filtros.append("ativo = %s")
        parametros.append(ativo)

    sql = "SELECT * FROM public.configuracao_planilhas"
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(parametros))
            return cursor.fetchall()


def buscar_configuracao_planilha(configuracao_planilha_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.configuracao_planilhas WHERE id = %s",
            (configuracao_planilha_id,),
            "Configuração de planilha não encontrada.",
        )


def alterar_configuracao_planilha(
    configuracao_planilha_id: int,
    **campos: Any,
) -> dict[str, Any]:
    campos_permitidos = {
        "nome_configuracao",
        "descricao",
        "caminho_arquivo",
        "nome_arquivo",
        "abas",
        "usuario_acesso",
        "senha_acesso",
        "tipo_arquivo",
        "possui_cabecalho",
        "linha_cabecalho",
        "intervalo_leitura",
        "coluna_data_referencia",
        "destino_entidade",
        "modo_carga",
        "ativo",
        "id_configuracao_inicial",
    }
    return _alterar_por_campos(
        tabela="configuracao_planilhas",
        registro_id=configuracao_planilha_id,
        campos=campos,
        campos_permitidos=campos_permitidos,
        possui_data_alteracao=True,
        mensagem="Configuração de planilha não encontrada.",
    )


def desativar_configuracao_planilha(configuracao_planilha_id: int) -> dict[str, Any]:
    return alterar_configuracao_planilha(configuracao_planilha_id, ativo=False)


# =============================================================================
# FONTES
# Uma fonte é filha de uma configuração relacional, API ou planilha.
# =============================================================================

def _validar_referencia_fonte(
    conn: psycopg.Connection,
    tipo_fonte: TipoFonte,
    id_referencia: int,
) -> dict[str, Any]:
    tipo_fonte = tipo_fonte.upper()

    if tipo_fonte not in TABELA_CONFIGURACAO_POR_TIPO:
        raise ValueError("tipo_fonte deve ser ODBC, API ou PLANILHA.")

    tabela = TABELA_CONFIGURACAO_POR_TIPO[tipo_fonte]

    configuracao = _buscar_um(
        conn,
        f"""
        SELECT id, id_configuracao_inicial, ativo
          FROM public.{tabela}
         WHERE id = %s
        """,
        (id_referencia,),
        f"Configuração do tipo {tipo_fonte} não encontrada.",
    )

    if configuracao["id_configuracao_inicial"] is None:
        raise ValueError(
            f"A configuração {tipo_fonte} não está vinculada a configuracao_pombo."
        )

    _validar_id(
        conn,
        "configuracao_pombo",
        configuracao["id_configuracao_inicial"],
        "Ambiente Pombo da configuração não encontrado.",
    )

    return configuracao


def criar_fonte(
    tipo_fonte: TipoFonte,
    id_referencia: int,
    nome_fonte: str,
    ordem_execucao: int = 1,
    grupo_execucao: int = 1,
    modo_carga: str = "INCREMENTAL",
    intervalo_minutos: int = 5,
    carga_inicial: bool = True,
    ativo: bool = True,
) -> dict[str, Any]:
    tipo_fonte = tipo_fonte.upper()

    with transacao() as conn:
        configuracao = _validar_referencia_fonte(
            conn,
            tipo_fonte,
            id_referencia,
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.fontes
                (
                    nome_fonte,
                    tipo_fonte,
                    id_referencia,
                    ordem_execucao,
                    grupo_execucao,
                    modo_carga,
                    intervalo_minutos,
                    carga_inicial,
                    ativo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    nome_fonte,
                    tipo_fonte,
                    id_referencia,
                    ordem_execucao,
                    grupo_execucao,
                    modo_carga,
                    intervalo_minutos,
                    carga_inicial,
                    ativo,
                ),
            )
            fonte = cursor.fetchone()
            fonte["id_configuracao_pombo"] = configuracao[
                "id_configuracao_inicial"
            ]
            return fonte


def listar_fontes(
    id_configuracao_pombo: int | None = None,
    tipo_fonte: TipoFonte | None = None,
    ativo: bool | None = None,
) -> list[dict[str, Any]]:
    filtros: list[str] = []
    parametros: list[Any] = []

    if tipo_fonte is not None:
        filtros.append("f.tipo_fonte = %s")
        parametros.append(tipo_fonte.upper())

    if ativo is not None:
        filtros.append("f.ativo = %s")
        parametros.append(ativo)

    if id_configuracao_pombo is not None:
        filtros.append(
            """
            (
                (f.tipo_fonte = 'ODBC' AND EXISTS (
                    SELECT 1
                      FROM public.configuracao c
                     WHERE c.id = f.id_referencia
                       AND c.id_configuracao_inicial = %s
                ))
                OR
                (f.tipo_fonte = 'API' AND EXISTS (
                    SELECT 1
                      FROM public.configuracao_api a
                     WHERE a.id = f.id_referencia
                       AND a.id_configuracao_inicial = %s
                ))
                OR
                (f.tipo_fonte = 'PLANILHA' AND EXISTS (
                    SELECT 1
                      FROM public.configuracao_planilhas p
                     WHERE p.id = f.id_referencia
                       AND p.id_configuracao_inicial = %s
                ))
            )
            """
        )
        parametros.extend(
            [
                id_configuracao_pombo,
                id_configuracao_pombo,
                id_configuracao_pombo,
            ]
        )

    sql = "SELECT f.* FROM public.fontes f"
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY f.ordem_execucao, f.id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(parametros))
            return cursor.fetchall()


def buscar_fonte(fonte_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.fontes WHERE id = %s",
            (fonte_id,),
            "Fonte não encontrada.",
        )


def alterar_fonte(
    fonte_id: int,
    tipo_fonte: TipoFonte,
    id_referencia: int,
    nome_fonte: str,
    ordem_execucao: int,
    grupo_execucao: int,
    modo_carga: str,
    intervalo_minutos: int,
    carga_inicial: bool,
    ativo: bool,
) -> dict[str, Any]:
    tipo_fonte = tipo_fonte.upper()

    with transacao() as conn:
        _validar_referencia_fonte(conn, tipo_fonte, id_referencia)

        return _buscar_um(
            conn,
            """
            UPDATE public.fontes
               SET nome_fonte = %s,
                   tipo_fonte = %s,
                   id_referencia = %s,
                   ordem_execucao = %s,
                   grupo_execucao = %s,
                   modo_carga = %s,
                   intervalo_minutos = %s,
                   carga_inicial = %s,
                   ativo = %s,
                   data_alteracao = CURRENT_TIMESTAMP
             WHERE id = %s
         RETURNING *
            """,
            (
                nome_fonte,
                tipo_fonte,
                id_referencia,
                ordem_execucao,
                grupo_execucao,
                modo_carga,
                intervalo_minutos,
                carga_inicial,
                ativo,
                fonte_id,
            ),
            "Fonte não encontrada.",
        )


def desativar_fonte(fonte_id: int) -> dict[str, Any]:
    with transacao() as conn:
        _validar_id(conn, "fontes", fonte_id, "Fonte não encontrada.")

        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE public.configuracao_entidades
                   SET ativo = FALSE
                 WHERE id_fonte = %s
                """,
                (fonte_id,),
            )

        return _buscar_um(
            conn,
            """
            UPDATE public.fontes
               SET ativo = FALSE,
                   data_alteracao = CURRENT_TIMESTAMP
             WHERE id = %s
         RETURNING *
            """,
            (fonte_id,),
            "Fonte não encontrada.",
        )


# =============================================================================
# CONFIGURACAO_ENTIDADES
# =============================================================================

def criar_entidade(
    id_fonte: int,
    nome_tabela: str,
    tipo_carga: str,
    intervalo_minutos: int,
    prioridade: int,
    tamanho_lote: int,
    usar_trigger: bool,
    ativo: bool = True,
) -> dict[str, Any]:
    with transacao() as conn:
        _validar_id(conn, "fontes", id_fonte, "Fonte não encontrada.")

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.configuracao_entidades
                    (
                        id_fonte,
                        nome_tabela,
                        tipo_carga,
                        intervalo_minutos,
                        prioridade,
                        tamanho_lote,
                        usar_trigger,
                        ativo
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        id_fonte,
                        nome_tabela,
                        tipo_carga,
                        intervalo_minutos,
                        prioridade,
                        tamanho_lote,
                        usar_trigger,
                        ativo,
                    ),
                )
                return cursor.fetchone()
        except psycopg.errors.UniqueViolation as exc:
            raise ValueError(
                "A entidade já está cadastrada para essa fonte."
            ) from exc


def listar_entidades(
    id_fonte: int | None = None,
    ativo: bool | None = None,
) -> list[dict[str, Any]]:
    filtros: list[str] = []
    parametros: list[Any] = []

    if id_fonte is not None:
        filtros.append("id_fonte = %s")
        parametros.append(id_fonte)

    if ativo is not None:
        filtros.append("ativo = %s")
        parametros.append(ativo)

    sql = "SELECT * FROM public.configuracao_entidades"
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY prioridade, id"

    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(parametros))
            return cursor.fetchall()


def buscar_entidade(entidade_id: int) -> dict[str, Any]:
    with hub_connect() as conn:
        return _buscar_um(
            conn,
            "SELECT * FROM public.configuracao_entidades WHERE id = %s",
            (entidade_id,),
            "Entidade não encontrada.",
        )


def alterar_entidade(
    entidade_id: int,
    nome_tabela: str,
    tipo_carga: str,
    intervalo_minutos: int,
    prioridade: int,
    tamanho_lote: int,
    usar_trigger: bool,
    ativo: bool,
) -> dict[str, Any]:
    with transacao() as conn:
        try:
            return _buscar_um(
                conn,
                """
                UPDATE public.configuracao_entidades
                   SET nome_tabela = %s,
                       tipo_carga = %s,
                       intervalo_minutos = %s,
                       prioridade = %s,
                       tamanho_lote = %s,
                       usar_trigger = %s,
                       ativo = %s
                 WHERE id = %s
             RETURNING *
                """,
                (
                    nome_tabela,
                    tipo_carga,
                    intervalo_minutos,
                    prioridade,
                    tamanho_lote,
                    usar_trigger,
                    ativo,
                    entidade_id,
                ),
                "Entidade não encontrada.",
            )
        except psycopg.errors.UniqueViolation as exc:
            raise ValueError(
                "A entidade já está cadastrada para essa fonte."
            ) from exc


def desativar_entidade(entidade_id: int) -> dict[str, Any]:
    with transacao() as conn:
        return _buscar_um(
            conn,
            """
            UPDATE public.configuracao_entidades
               SET ativo = FALSE
             WHERE id = %s
         RETURNING *
            """,
            (entidade_id,),
            "Entidade não encontrada.",
        )


# =============================================================================
# ALTERAÇÃO DINÂMICA SEGURA PARA API E PLANILHAS
# =============================================================================

def _alterar_por_campos(
    tabela: str,
    registro_id: int,
    campos: dict[str, Any],
    campos_permitidos: set[str],
    possui_data_alteracao: bool,
    mensagem: str,
) -> dict[str, Any]:
    if not campos:
        raise ValueError("Nenhum campo foi informado para alteração.")

    campos_invalidos = set(campos) - campos_permitidos
    if campos_invalidos:
        raise ValueError(
            "Campos inválidos: " + ", ".join(sorted(campos_invalidos))
        )

    if "id_configuracao_inicial" in campos:
        with hub_connect() as conn:
            _validar_id(
                conn,
                "configuracao_pombo",
                int(campos["id_configuracao_inicial"]),
                "Ambiente Pombo não encontrado.",
            )

    atribuicoes = [f"{campo} = %s" for campo in campos]
    valores = list(campos.values())

    if possui_data_alteracao:
        atribuicoes.append("data_alteracao = CURRENT_TIMESTAMP")

    valores.append(registro_id)

    with transacao() as conn:
        return _buscar_um(
            conn,
            f"""
            UPDATE public.{tabela}
               SET {', '.join(atribuicoes)}
             WHERE id = %s
         RETURNING *
            """,
            tuple(valores),
            mensagem,
        )


# =============================================================================
# VISÃO COMPLETA DO AMBIENTE
# =============================================================================

def buscar_estrutura_ambiente(ambiente_id: int) -> dict[str, Any]:
    ambiente = buscar_ambiente(ambiente_id)
    relacionais = listar_configuracoes_relacionais(ambiente_id)
    apis = listar_configuracoes_api(ambiente_id)
    planilhas = listar_configuracoes_planilhas(ambiente_id)
    fontes = listar_fontes(id_configuracao_pombo=ambiente_id)

    ids_fontes = [fonte["id"] for fonte in fontes]
    entidades: list[dict[str, Any]] = []

    if ids_fontes:
        with hub_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                      FROM public.configuracao_entidades
                     WHERE id_fonte = ANY(%s)
                     ORDER BY id_fonte, prioridade, id
                    """,
                    (ids_fontes,),
                )
                entidades = cursor.fetchall()

    return {
        "ambiente": ambiente,
        "configuracoes": {
            "relacionais": relacionais,
            "apis": apis,
            "planilhas": planilhas,
        },
        "fontes": fontes,
        "entidades": entidades,
    }


# =============================================================================
# TESTE DE CONEXÃO
# =============================================================================

def testar_conexao() -> bool:
    with hub_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS conectado")
            resultado = cursor.fetchone()
            return resultado["conectado"] == 1


if __name__ == "__main__":
    try:
        if testar_conexao():
            print("Conexão com o banco central do Pombo realizada com sucesso.")
            print("Ambientes cadastrados:")
            for ambiente in listar_ambientes():
                print(ambiente)
    except Exception as erro:
        print(f"Erro no Pombo Configurador: {erro}")