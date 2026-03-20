## 2025-03-20 - Vectorize Pandas string operations
**Learning:** Using `.apply(axis=1)` for string formatting and concatenation over large DataFrames (like `tabelas_auditorias/processing.py`) creates a huge performance bottleneck because it relies on Python-level loops rather than C-optimized paths.
**Action:** Always prioritize vectorization (e.g., using `.astype(str)` and `+` concatenation, or `.str.zfill()`) over `.apply(axis=1)` when combining or formatting strings in this repository.
