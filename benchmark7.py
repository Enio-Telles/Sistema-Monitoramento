import time
import polars as pl
from pathlib import Path
from fiscal_app.services.aggregation_service import AggregationService

n_rows = 100000
df = pl.DataFrame({
    "descrição_normalizada": [f"norm_{i}" if i % 2 == 0 else None for i in range(n_rows)],
    "descricao": [f"desc_{i}" if i % 3 == 0 else None for i in range(n_rows)],
    "verificado": [False for _ in range(n_rows)]
})

target_dir = Path("/tmp/cnpj_dir_7")
target_dir.mkdir(parents=True, exist_ok=True)
(target_dir / "produtos").mkdir(parents=True, exist_ok=True)
target = target_dir / "produtos" / "tabela_produtos_editavel_123.parquet"
df.write_parquet(target)

current = pl.read_parquet(target)
rows = [
    {"descrição_normalizada": f"norm_{i}" if i % 2 == 0 else None, "descricao": f"desc_{i}" if i % 3 == 0 else None}
    for i in range(10, 20)
]

removed_keys = [
    (str(row.get("descrição_normalizada") or ""), str(row.get("descricao") or ""))
    for row in rows
]

start = time.time()
key_set = set(removed_keys)
kept_rows = []
for row in current.iter_rows(named=True):
    key = (str(row.get("descrição_normalizada") or ""), str(row.get("descricao") or ""))
    if key not in key_set:
        kept_rows.append(row)
kept_rows = pl.DataFrame(kept_rows)
end = time.time()
print(f"Original Time taken: {end - start:.4f} seconds")


start = time.time()
if not removed_keys:
    kept_df = current
else:
    # Use tuple is_in

    kept_df = current.filter(
        ~pl.struct(
            pl.col("descrição_normalizada").fill_null("").cast(pl.String),
            pl.col("descricao").fill_null("").cast(pl.String)
        ).is_in(removed_keys)
    )

end = time.time()
print(f"Struct list of tuples is_in Approach Time taken: {end - start:.4f} seconds")
print(f"Original kept rows: {kept_rows.shape[0]}, New kept rows: {kept_df.shape[0]}")
