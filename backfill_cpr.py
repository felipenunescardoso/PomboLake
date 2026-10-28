"""
backfill_tipo_cpr.py
Corrigir Tipo (AR/RC) dos POSTOS em contas_pagar_receber que ficaram NULL.
Idempotente e tolerante a falhas anteriores.
"""
import os
from pathlib import Path

import duckdb
import pandas as pd
import psycopg
from psycopg.rows import dict_row

SILVER_CURRENT = Path(r"\\10.68.152.218\d$\Pipeline\Silver\Current\contas_pagar_receber.parquet")
GOLD_PARQUET   = Path(r"\\10.68.152.218\d$\Pipeline\Gold\contas_pagar_receber.parquet")

def log(msg):
    print(f"[{pd.Timestamp.now():%Y-%m-%d %H:%M:%S}] {msg}")

def corrigir_tipo(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    if "nome_alias" not in df.columns or "tipo" not in df.columns:
        log(f"{nome}: colunas nome_alias/tipo não encontradas, pulando")
        return df, 0

    mask_postos = df["nome_alias"].astype(str).str.upper().eq("POSTOS")
    mask_null   = df["tipo"].isna() | (df["tipo"].astype(str).str.strip() == "")
    alvo        = mask_postos & mask_null

    if not alvo.any():
        log(f"{nome}: nenhum POSTOS com Tipo NULL (já corrigido).")
        return df, 0

    if "pendente" in df.columns:
        pendente = df.loc[alvo, "pendente"]
        df.loc[alvo, "tipo"] = pendente.map({True: "AR", False: "RC"}).fillna(df.loc[alvo, "tipo"])
        log(f"{nome}: {alvo.sum()} registro(s) corrigido(s)")
    else:
        log(f"{nome}: 'pendente' ausente, sem como derivar Tipo")

    return df, alvo.sum()

def salvar_com_backup(df: pd.DataFrame, original: Path, nome: str):
    tmp = original.with_name(f"tmp_{original.stem}_backfill.parquet")
    backup = original.with_name(f"{original.stem}_pre_backfill.parquet")

    df.to_parquet(tmp, compression="zstd", index=False)
    log(f"{nome}: tmp gravado ({len(df)} linhas)")

    # Se backup não existe, move original para backup
    if not backup.exists():
        original.rename(backup)
        log(f"{nome}: backup criado -> {backup.name}")
    else:
        # Backup já existe, apaga original para liberar o nome
        original.unlink()
        log(f"{nome}: backup já existia, original removido")

    tmp.rename(original)
    log(f"{nome}: arquivo atualizado")

def sincronizar_hub(df_gold: pd.DataFrame):
    df_postos = df_gold[df_gold["nome_alias"].astype(str).str.upper().eq("POSTOS")].copy()
    if df_postos.empty:
        log("Hub: nenhum POSTOS no Gold, nada a sincronizar")
        return

    log(f"Hub: sincronizando {len(df_postos)} linhas POSTOS...")

    hub = psycopg.connect(
        host=os.getenv("HUB_CONTROLE_HOST"),
        port=int(os.getenv("HUB_CONTROLE_PORT", "54320")),
        dbname=os.getenv("HUB_DW_DB", "hub_dw"),
        user=os.getenv("HUB_DW_USER"),
        password=os.getenv("HUB_DW_PASSWORD"),
        row_factory=dict_row,
    )

    try:
        with hub.transaction():
            with hub.cursor() as cur:
                cur.execute("""
                    DELETE FROM gold.contas_pagar_receber
                    WHERE nome_alias = 'POSTOS' AND tipo IS NULL
                """)
                n_delete = cur.rowcount
                log(f"Hub: {n_delete} linha(s) POSTOS com tipo NULL removida(s)")

                colunas_hub = [c for c in df_postos.columns if c in (
                    "nome_alias","cod_empresa","tipo","num_lancamento","cod_ent","num_docto",
                    "num_lote","moeda","valor_orig","saldo","historico","seq_nf","parcela",
                    "cod_tp_cobr","cod_rec","rz_social","observ","valor_pago","numero_nf",
                    "num_pedido","cc_entidade","cc_custo_venda","cc_estoque","custo",
                    "cc_dsp_com","cc_com_a_pg","val_comissao","usuario","comprador",
                    "rz_social_comprador","sinistro","tx_juros_dia","nosso_numero",
                    "baixa_eletronica","baixa_confirmada","postergacao_confirmada",
                    "seq_cc","processado_pela_tabela_preco","competencia","convenio",
                    "cartorio","previsao","seq_contabilizacao","pode_contabilizar",
                    "seq_lote_real","seq_padrao","seq_docto","banco","agencia","conta",
                    "contador_seq_docto","valor_bruto","sequencial_reneg","renegociado",
                    "info_adic","comando_auxiliar","cheque_devolvido","gera_adiantamento",
                    "contabiliza_real","fat_d_fin","val_fat","autorizado","autorizado_por",
                    "autorizado_em","dig_agencia","dig_conta","cr_nosso_numero","seq_banco",
                    "cod_empresa_adto","seq_credito_adto","com_adto_quantificado",
                    "usuario_cancelamento","data_cancelamento","seq_contrato",
                    "data_inclusao","carteira","valor_acrescimo_tipo_cobr",
                    "data_cancelamento_lote","seq_cpr_recorrente","utiliza_conversao",
                    "total_parcelas","sequencial_processo_pmm","sequencial_etapa_pmm",
                    "valor_contas","cnpj","data_emissao","data_vencimento_original",
                    "data_vencimento","data_cancelamento_key","cpr_key","empresa_key",
                    "entidade_key","nota_fiscal_key","tipo_documento_key","centro_custo_key",
                    "lote_key","cod_tp_cobr_key","venda_codigo","duplicata_codigo",
                    "tipo_original","data_pagamento","plano_conta_gerencial_codigo",
                    "convertido","titulo_numero","data_hora_atualizacao","codigo",
                    "data_emissao_key","data_vencimento_original_key","data_vencimento_key"
                )]

                if not colunas_hub:
                    log("Hub: nenhuma coluna conhecida, abortando insert")
                    return

                dados = []
                for _, row in df_postos[colunas_hub].iterrows():
                    dados.append([None if pd.isna(v) else v for v in row.tolist()])
                placeholders = ",".join(["%s"] * len(colunas_hub))
                sql = f"""
                    INSERT INTO gold.contas_pagar_receber ({",".join(colunas_hub)})
                    VALUES ({placeholders})
                    ON CONFLICT (cpr_key) DO UPDATE SET tipo = EXCLUDED.tipo
                """
                cur.executemany(sql, dados)
                log(f"Hub: {len(dados)} linha(s) POSTOS inserida(s)/atualizada(s)")

        hub.commit()
        log("Hub: commit concluído")
    except Exception as e:
        hub.rollback()
        log(f"Hub: ERRO, rollback: {e}")
        raise
    finally:
        hub.close()

def main():
    duck = duckdb.connect()

    log("Lendo Silver Current...")
    df_silver = duck.execute(f"SELECT * FROM read_parquet('{SILVER_CURRENT.as_posix()}', union_by_name=true)").df()
    df_silver, n_silver = corrigir_tipo(df_silver, "Silver")
    if n_silver > 0:
        salvar_com_backup(df_silver, SILVER_CURRENT, "Silver")
    else:
        log("Silver: já está OK, sem escrita")

    log("Lendo Gold...")
    df_gold = duck.execute(f"SELECT * FROM read_parquet('{GOLD_PARQUET.as_posix()}', union_by_name=true)").df()
    df_gold, n_gold = corrigir_tipo(df_gold, "Gold")
    if n_gold > 0:
        salvar_com_backup(df_gold, GOLD_PARQUET, "Gold")
    else:
        log("Gold: já está OK, sem escrita")

    sincronizar_hub(df_gold)
    log("Concluído com sucesso.")

if __name__ == "__main__":
    main()