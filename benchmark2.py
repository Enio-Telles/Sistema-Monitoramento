import time
import polars as pl
from pathlib import Path
from fiscal_app.services.aggregation_service import AggregationService

n_rows = 100000
df = pl.DataFrame({
    "descrição_normalizada": [f"norm_{i}" if i % 2 == 0 else None for i in range(n_rows)],
    "descricao": [f"desc_{i}" if i % 3 == 0 else None for i in range(n_rows)],
})

target_dir = Path("/tmp/cnpj_dir_2")
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
end = time.time()
print(f"Original Time taken: {end - start:.4f} seconds")

start = time.time()
# New approach using antijoin
keys_to_remove_df = pl.DataFrame(
    removed_keys,
    schema=["descrição_normalizada", "descricao"],
    orient="row"
)

# the original code replaces None with ""
current_with_clean_keys = current.with_columns(
    pl.col("descrição_normalizada").fill_null("").cast(pl.String).alias("_key_norm"),
    pl.col("descricao").fill_null("").cast(pl.String).alias("_key_desc")
)
keys_to_remove_df = keys_to_remove_df.with_columns(
    pl.col("descrição_normalizada").cast(pl.String).alias("_key_norm"),
    pl.col("descricao").cast(pl.String).alias("_key_desc")
)

kept_df = current_with_clean_keys.join(
    keys_to_remove_df,
    on=["_key_norm", "_key_desc"],
    how="anti"
).drop(["_key_norm", "_key_desc"])

kept_rows_new = kept_df.to_dicts()

end = time.time()
print(f"New Approach Time taken: {end - start:.4f} seconds")
print(f"Original kept rows: {len(kept_rows)}, New kept rows: {len(kept_df)}")
