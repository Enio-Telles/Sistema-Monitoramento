import time
import polars as pl
from pathlib import Path

n_rows = 100000
df = pl.DataFrame({
    "descrição_normalizada": [f"norm_{i}" if i % 2 == 0 else None for i in range(n_rows)],
    "descricao": [f"desc_{i}" if i % 3 == 0 else None for i in range(n_rows)],
    "verificado": [False for _ in range(n_rows)]
})

target_dir = Path("/tmp/cnpj_dir_9")
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

aggregated_row = {
    "descrição_normalizada": "aggr_norm",
    "descricao": "aggr_desc",
    "verificado": True
}

start = time.time()
key_set = set(removed_keys)
kept_rows = []
for row in current.iter_rows(named=True):
    key = (str(row.get("descrição_normalizada") or ""), str(row.get("descricao") or ""))
    if key not in key_set:
        kept_rows.append(row)
updated_rows = kept_rows + [aggregated_row]
updated1 = pl.DataFrame(updated_rows, schema=current.schema)
end = time.time()
print(f"Original Time taken: {end - start:.4f} seconds")


start = time.time()
if not removed_keys:
    kept_df = current
else:
    # Use tuple is_in
    removed_dicts = [{"descrição_normalizada": k[0], "descricao": k[1]} for k in removed_keys]

    kept_df = current.filter(
        ~pl.struct(
            pl.col("descrição_normalizada").fill_null("").cast(pl.String),
            pl.col("descricao").fill_null("").cast(pl.String)
        ).is_in(removed_dicts)
    )

aggregated_df = pl.DataFrame([aggregated_row], schema=current.schema)
updated2 = pl.concat([kept_df, aggregated_df])

end = time.time()
print(f"Struct list of dicts is_in + concat Approach Time taken: {end - start:.4f} seconds")
print(f"Original shape: {updated1.shape}, New shape: {updated2.shape}")

print(updated1.tail(2))
print(updated2.tail(2))
