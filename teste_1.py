import pandas as pd
import glob

# nome truncado no print; glob resolve sem precisar do nome exato
candidatos = glob.glob(
    r"D:\Pipeline\Bronze\bdp_4000\itensnf\ingestion_year=2026\ingestion_month=09\ingestion_day=30\*.parquet"
)
arquivo = candidatos[0]
print("Arquivo:", arquivo)

df = pd.read_parquet(arquivo)

print("=== COLUNAS (nome | tipo | nao-nulos) ===")
for col in df.columns:
    nn = df[col].notna().sum()
    print(f"{col} | {df[col].dtype} | {nn}/{len(df)}")

print("\n=== AMOSTRA (2 linhas, transposta) ===")
with pd.option_context("display.max_columns", None, "display.width", 250):
    print(df.head(2).T)