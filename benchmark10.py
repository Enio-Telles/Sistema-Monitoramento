import time
import polars as pl
from pathlib import Path
from fiscal_app.services.aggregation_service import AggregationService

n_rows = 100000
df = pl.DataFrame({
    "descrição_normalizada": [f"norm_{i}" if i % 2 == 0 else None for i in range(n_rows)],
    "descricao": [f"desc_{i}" if i % 3 == 0 else None for i in range(n_rows)],
    "lista_codigos": [[f"[{i}; 1]"] for i in range(n_rows)],
    "lista_tipo_item": [[] for _ in range(n_rows)],
    "lista_ncm": [[] for _ in range(n_rows)],
    "lista_cest": [[] for _ in range(n_rows)],
    "lista_gtin": [[] for _ in range(n_rows)],
    "lista_unid": [[] for _ in range(n_rows)],
    "lista_descricoes": [[] for _ in range(n_rows)],
    "lista_descricoes_normalizadas": [[] for _ in range(n_rows)],
    "codigo_padrao": [f"{i}" for i in range(n_rows)],
    "qtd_codigos": [1 for _ in range(n_rows)],
    "tipo_item_padrao": [None for _ in range(n_rows)],
    "NCM_padrao": [None for _ in range(n_rows)],
    "CEST_padrao": [None for _ in range(n_rows)],
    "GTIN_padrao": [None for _ in range(n_rows)],
    "verificado": [False for _ in range(n_rows)],
    "descricao_padrao": [None for _ in range(n_rows)]
})

target_dir = Path("/tmp/cnpj_dir_10")
target_dir.mkdir(parents=True, exist_ok=True)
(target_dir / "produtos").mkdir(parents=True, exist_ok=True)
target = target_dir / "produtos" / "tabela_produtos_editavel_123.parquet"
df.write_parquet(target)

current = pl.read_parquet(target)
rows = [
    {"descrição_normalizada": f"norm_{i}" if i % 2 == 0 else None, "descricao": f"desc_{i}" if i % 3 == 0 else None, "lista_codigos": [f"[{i}; 1]"]}
    for i in range(10, 20)
]

service = AggregationService()

start = time.time()
service.aggregate_rows(target_dir, "123", rows, "new desc", "new norm")
end = time.time()

print(f"Post-Optimization execution time: {end - start:.4f} seconds")
