import duckdb

arquivo = r"D:\Pipeline\Silver\Current\contaspagarreceber.parquet"

con = duckdb.connect()

print("Total de registros:")
print(
    con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
    """, [arquivo]).fetchone()[0]
)

print("\nContabilizaNaNF:")
resultado = con.execute("""
    SELECT
        "ContabilizaNaNF",
        COUNT(*) AS quantidade
    FROM read_parquet(?)
    GROUP BY "ContabilizaNaNF"
    ORDER BY "ContabilizaNaNF"
""", [arquivo]).fetchall()

for valor, quantidade in resultado:
    print(f"{valor}: {quantidade}")

print("\nOperações:")
resultado = con.execute("""
    SELECT
        _operation,
        COUNT(*) AS quantidade
    FROM read_parquet(?)
    GROUP BY _operation
    ORDER BY _operation
""", [arquivo]).fetchall()

for operacao, quantidade in resultado:
    print(f"{operacao}: {quantidade}")