# import pyarrow.parquet as pq

# arquivo = r"\\10.68.152.218\d$\Pipeline\Silver\itensnf.parquet"

# df = pq.read_table(arquivo).to_pandas()

# print("Colunas:")
# print(df.columns.tolist())

# print("\nLinhas com Item nulo:")
# print(df[df["Item"].isna()].head(20).to_string())

from pathlib import Path
import pyarrow.parquet as pq

arquivo = Path(
    r"\\10.68.152.218\d$\Pipeline\Bronze\bdp_500\itensnf"
    r"\ingestion_year=2026"
    r"\ingestion_month=07"
    r"\ingestion_day=17"
    r"\full_000001_20260717_161412_574091_17f5395a.parquet"
)

df = pq.read_table(str(arquivo)).to_pandas()

print("Quantidade de Item nulo:")
print(df["Item"].isna().sum())

print("\nLinhas com Item nulo:")
print(df[df["Item"].isna()].head(20).to_string())



#D:\Pipeline\Bronze\bdp_500\itensnf\ingestion_year=2026\ingestion_month=07\ingestion_day=17