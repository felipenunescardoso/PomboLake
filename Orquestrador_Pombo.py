from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import sql


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PASTA_GOLD = Path(r"\\10.68.152.218\d$\Pipeline\Gold")
SCHEMA_DESTINO = "gold"
TAMANHO_LOTE = 10_000

# Nome do arquivo Parquet -> nome da tabela PostgreSQL
MAPEAMENTO_TABELAS: dict[str, str] = {
    "contaspagarreceber.parquet": "contaspagarreceber",
    "contaspagarreceberMoeda.parquet": "contaspagarrecebermoeda",
    "notafiscal.parquet": "notafiscal",
    "itensnf.parquet": "itensnf",
    "MovimentoPlanoContas.parquet": "movimentoplanocontas",
    "CentroCustoRateio.parquet": "centrocustorateio",
    "CentroCustoPagar.parquet": "centrocustopagar",
    "Z_NotaFiscal_ClassificacaoDespesas.parquet": (
        "z_notafiscal_classificacaodespesas"
    ),
    "empresas.parquet": "empresas",
    "Entidade.parquet": "entidade",
    "Vendedor.parquet": "vendedor",
    "Portador.parquet": "portador",
    "TipoDocumento.parquet": "tipodocumento",
    "NaturezaOperacao.parquet": "naturezaoperacao",
    "z_classificacaodespesas.parquet": "z_classificacaodespesas",
    "planocontas.parquet": "planocontas",
    "CentroCusto.parquet": "centrocusto",
}


@dataclass(frozen=True)
class ConfigPostgres:
    host: str
    port: int
    dbname: str
    user: str
    password: str


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


def carregar_config_postgres() -> ConfigPostgres:
    """
    Reutiliza o mesmo host e porta usados pelo Pombo.

    Variáveis esperadas:
        HUB_CONTROLE_HOST
        HUB_CONTROLE_PORT (opcional, padrão 54320)
        HUB_DW_USER
        HUB_DW_PASSWORD
    """
    valores = {
        "HUB_CONTROLE_HOST": os.getenv("HUB_CONTROLE_HOST", "").strip(),
        "HUB_CONTROLE_PORT": os.getenv("HUB_CONTROLE_PORT", "54320").strip(),
        "HUB_DW_USER": os.getenv("HUB_DW_USER", "").strip(),
        "HUB_DW_PASSWORD": os.getenv("HUB_DW_PASSWORD", ""),
    }

    obrigatorias = [
        "HUB_CONTROLE_HOST",
        "HUB_DW_USER",
        "HUB_DW_PASSWORD",
    ]

    faltantes = [nome for nome in obrigatorias if not valores[nome]]

    if faltantes:
        raise RuntimeError(
            "Variáveis de ambiente ausentes: " + ", ".join(faltantes)
        )

    try:
        porta = int(valores["HUB_CONTROLE_PORT"])
    except ValueError as exc:
        raise RuntimeError(
            "HUB_CONTROLE_PORT deve ser um número inteiro."
        ) from exc

    return ConfigPostgres(
        host=valores["HUB_CONTROLE_HOST"],
        port=porta,
        dbname="hub_dw",
        user=valores["HUB_DW_USER"],
        password=valores["HUB_DW_PASSWORD"],
    )


def conectar_postgres(config: ConfigPostgres) -> psycopg.Connection[Any]:
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        connect_timeout=15,
        application_name="Pombo_Orquestrador_DW",
    )


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
        return [linha[0] for linha in cursor.fetchall()]


def normalizar_valor(valor: Any) -> Any:
    """
    Converte valores do PyArrow para tipos aceitos pelo psycopg.
    """
    if isinstance(valor, pa.Scalar):
        return valor.as_py()

    return valor


def gerar_linhas(
    lote: pa.RecordBatch,
    colunas: list[str],
) -> Iterator[tuple[Any, ...]]:
    dados = lote.to_pydict()

    for indice in range(lote.num_rows):
        yield tuple(
            normalizar_valor(dados[coluna][indice])
            for coluna in colunas
        )


def validar_colunas(
    arquivo_parquet: Path,
    colunas_parquet: list[str],
    colunas_postgres: list[str],
    tabela: str,
) -> None:
    conjunto_parquet = set(colunas_parquet)
    conjunto_postgres = set(colunas_postgres)

    faltando_no_parquet = [
        coluna for coluna in colunas_postgres
        if coluna not in conjunto_parquet
    ]
    sobrando_no_parquet = [
        coluna for coluna in colunas_parquet
        if coluna not in conjunto_postgres
    ]

    if faltando_no_parquet or sobrando_no_parquet:
        partes: list[str] = [
            f"Incompatibilidade entre {arquivo_parquet.name} e "
            f"{SCHEMA_DESTINO}.{tabela}."
        ]

        if faltando_no_parquet:
            partes.append(
                "Colunas ausentes no Parquet: "
                + ", ".join(faltando_no_parquet)
            )

        if sobrando_no_parquet:
            partes.append(
                "Colunas extras no Parquet: "
                + ", ".join(sobrando_no_parquet)
            )

        raise RuntimeError(" ".join(partes))


def contar_linhas_postgres(
    conexao: psycopg.Connection[Any],
    schema: str,
    tabela: str,
) -> int:
    comando = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(tabela),
    )

    with conexao.cursor() as cursor:
        cursor.execute(comando)
        resultado = cursor.fetchone()
        return int(resultado[0]) if resultado else 0


def carregar_parquet_na_tabela(
    conexao: psycopg.Connection[Any],
    arquivo_parquet: Path,
    tabela: str,
) -> int:
    parquet = pq.ParquetFile(arquivo_parquet)
    colunas_parquet = parquet.schema_arrow.names

    colunas_postgres = listar_colunas_postgres(
        conexao,
        SCHEMA_DESTINO,
        tabela,
    )

    if not colunas_postgres:
        raise RuntimeError(
            f"Tabela {SCHEMA_DESTINO}.{tabela} não encontrada."
        )

    validar_colunas(
        arquivo_parquet,
        colunas_parquet,
        colunas_postgres,
        tabela,
    )

    # Mantém exatamente a ordem física das colunas da tabela PostgreSQL.
    colunas_carga = colunas_postgres

    comando_truncate = sql.SQL("TRUNCATE TABLE {}.{}").format(
        sql.Identifier(SCHEMA_DESTINO),
        sql.Identifier(tabela),
    )

    comando_copy = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(SCHEMA_DESTINO),
        sql.Identifier(tabela),
        sql.SQL(", ").join(
            sql.Identifier(coluna) for coluna in colunas_carga
        ),
    )

    total_inserido = 0

    with conexao.cursor() as cursor:
        cursor.execute(comando_truncate)

        with cursor.copy(comando_copy) as copia:
            for lote in parquet.iter_batches(
                batch_size=TAMANHO_LOTE,
                columns=colunas_carga,
            ):
                for linha in gerar_linhas(lote, colunas_carga):
                    copia.write_row(linha)
                    total_inserido += 1

    return total_inserido


def validar_arquivos() -> None:
    if not PASTA_GOLD.exists():
        raise FileNotFoundError(
            f"Pasta Gold não encontrada ou sem acesso: {PASTA_GOLD}"
        )

    ausentes = [
        nome_arquivo
        for nome_arquivo in MAPEAMENTO_TABELAS
        if not (PASTA_GOLD / nome_arquivo).is_file()
    ]

    if ausentes:
        raise FileNotFoundError(
            "Arquivos Parquet ausentes: " + ", ".join(ausentes)
        )


def executar_pipeline() -> None:
    inicio_pipeline = time.perf_counter()

    configurar_logging()
    logging.info("Iniciando Pombo Orquestrador DW.")
    logging.info("Pasta Gold: %s", PASTA_GOLD)

    validar_arquivos()
    config = carregar_config_postgres()

    total_geral = 0

    with conectar_postgres(config) as conexao:
        logging.info(
            "Conectado ao PostgreSQL %s:%s/%s como %s.",
            config.host,
            config.port,
            config.dbname,
            config.user,
        )

        for nome_arquivo, tabela in MAPEAMENTO_TABELAS.items():
            arquivo = PASTA_GOLD / nome_arquivo
            inicio_tabela = time.perf_counter()

            logging.info(
                "Carregando %s -> %s.%s",
                arquivo.name,
                SCHEMA_DESTINO,
                tabela,
            )

            try:
                # Cada tabela possui sua própria transação.
                with conexao.transaction():
                    quantidade = carregar_parquet_na_tabela(
                        conexao,
                        arquivo,
                        tabela,
                    )

                    quantidade_validada = contar_linhas_postgres(
                        conexao,
                        SCHEMA_DESTINO,
                        tabela,
                    )

                    if quantidade != quantidade_validada:
                        raise RuntimeError(
                            f"Validação falhou em {SCHEMA_DESTINO}.{tabela}: "
                            f"{quantidade} linhas enviadas e "
                            f"{quantidade_validada} encontradas."
                        )

                duracao = time.perf_counter() - inicio_tabela
                total_geral += quantidade

                logging.info(
                    "SUCESSO | %s.%s | %s linhas | %.2f segundos",
                    SCHEMA_DESTINO,
                    tabela,
                    quantidade,
                    duracao,
                )

            except Exception:
                logging.exception(
                    "ERRO ao carregar %s em %s.%s. "
                    "A transação da tabela foi revertida.",
                    arquivo.name,
                    SCHEMA_DESTINO,
                    tabela,
                )
                raise

    duracao_total = time.perf_counter() - inicio_pipeline

    logging.info(
        "Pipeline concluída | %s tabelas | %s linhas | %.2f segundos",
        len(MAPEAMENTO_TABELAS),
        total_geral,
        duracao_total,
    )


if __name__ == "__main__":
    try:
        executar_pipeline()
    except KeyboardInterrupt:
        logging.error("Execução interrompida pelo usuário.")
        sys.exit(130)
    except Exception:
        logging.exception("Pipeline encerrada com erro.")
        sys.exit(1)