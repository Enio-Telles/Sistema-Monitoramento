#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import socket
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

try:
    import oracledb
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "O pacote 'oracledb' não está instalado. Instale as dependências com: pip install -r requirements_pipeline_oracle_parquet.txt"
    ) from exc

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Pacotes necessários ('pyarrow', 'pandas') não estão instalados. Instale as dependências."
    ) from exc

from tabelas_auditorias.processing import materializar_tabelas_consolidacao
from tabelas_auditorias.utils import normalize_df_types


@dataclass
class ExtractionResult:
    nome_consulta: str
    arquivo_sql: Path
    arquivo_saida: Path
    linhas: int


def log(msg: str) -> None:
    agora = datetime.now().strftime("%H:%M:%S")
    print(f"[{agora}] {msg}")


def sanitize_cnpj(cnpj: str) -> str:
    digits = re.sub(r"\D", "", cnpj or "")
    if not digits:
        raise ValueError("Informe um CNPJ válido.")
    return digits


def carregar_env() -> None:
    # Procura .env no diretório atual ou no diretório do script
    root = Path(__file__).resolve().parent
    candidatos = [
        Path.cwd() / ".env",
        root / ".env",
        root.parent / ".env",
    ]
    for arq in candidatos:
        if arq.exists():
            load_dotenv(arq, override=False, encoding="latin-1")
            break


def ler_sql(path: str | Path) -> str:
    path = Path(path)
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "cp1250"]
    for enc in encodings:
        try:
            return path.read_text(encoding=enc).strip().rstrip(";")
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Não foi possível ler o SQL: {path}")


def conectar_oracle(usuario: str | None = None, senha: str | None = None):
    carregar_env()
    host = os.getenv("ORACLE_HOST", "exa01-scan.sefin.ro.gov.br").strip()
    porta = int(os.getenv("ORACLE_PORT", "1521").strip())
    servico = os.getenv("ORACLE_SERVICE", "sefindw").strip()
    usuario = (usuario or os.getenv("DB_USER", "")).strip()
    senha = (senha or os.getenv("DB_PASSWORD", "")).strip()

    if not usuario or not senha:
        raise RuntimeError("Credenciais Oracle não encontradas. Preencha DB_USER e DB_PASSWORD no .env")

    dsn = oracledb.makedsn(host, porta, service_name=servico)
    try:
        ip = socket.gethostbyname(host)
        log(f"Host Oracle resolvido: {host} -> {ip}")
    except Exception:
        log(f"Aviso: não foi possível resolver DNS de {host}")

    try:
        conn = oracledb.connect(user=usuario, password=senha, dsn=dsn)
        with conn.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        return conn
    except socket.gaierror as exc:
        raise RuntimeError(
            f"Falha de conexão: não foi possível localizar o servidor '{host}'.\n"
            "Verifique se você está conectado à VPN da SEFIN ou se o endereço no .env está correto."
        ) from exc
    except oracledb.Error as exc:
        raise RuntimeError(f"Erro de banco de dados Oracle: {exc}") from exc


def extract_bind_names(sql: str) -> list[str]:
    # Identifica :NOME, mas ignora [:alnum:] (POSIX) usando lookbehind negativo
    names = re.findall(r"(?<!\[):([A-Za-z_][A-Za-z0-9_]*)", sql)
    seen = set()
    ordered = []
    for name in names:
        low = name.lower()
        if low not in seen:
            seen.add(low)
            ordered.append(low)
    return ordered


def build_binds(sql: str, params: dict[str, Any]) -> dict[str, Any]:
    provided = {k.lower(): v for k, v in params.items()}
    binds = {}
    for name in extract_bind_names(sql):
        binds[name] = provided.get(name.lower())
    return binds


def fetch_query_to_parquet(
    conn,
    sql: str,
    binds: dict[str, Any],
    output_path: Path,
    fetch_size: int = 50_000,
    source_name: str | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    writer: pq.ParquetWriter | None = None
    columns: list[str] = []

    with conn.cursor() as cursor:
        cursor.arraysize = fetch_size
        cursor.prefetchrows = fetch_size
        cursor.execute(sql, binds)
        columns = [desc[0] for desc in cursor.description]

        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break

            chunk = pd.DataFrame.from_records(rows, columns=columns)
            if source_name:
                chunk["fonte"] = source_name
            chunk = normalize_df_types(chunk)
            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
            writer.write_table(table)
            rows_written += len(chunk)

    if writer is not None:
        writer.close()
    else:
        empty_df = pd.DataFrame(columns=columns)
        pq.write_table(pa.Table.from_pandas(empty_df, preserve_index=False), output_path, compression="snappy")

    return rows_written


def discover_sql_files(sql_dir: Path) -> list[Path]:
    files = [p for p in sql_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sql"]
    files.sort(key=lambda p: p.name.lower())
    return files


def output_name_for_sql(sql_file: Path, cnpj: str) -> str:
    return f"{sql_file.stem.lower()}_{cnpj}.parquet"


def extrair_consultas(
    sql_dir: Path,
    pasta_cnpj: Path,
    cnpj: str,
    fetch_size: int,
    usuario: str | None,
    senha: str | None,
    data_limite: str | None = None,
) -> list[ExtractionResult]:
    conn = conectar_oracle(usuario=usuario, senha=senha)
    results: list[ExtractionResult] = []
    try:
        for sql_file in discover_sql_files(sql_dir):
            output_path = pasta_cnpj / output_name_for_sql(sql_file, cnpj)
            sql = ler_sql(sql_file)
            binds = build_binds(
                sql,
                {
                    "CNPJ": cnpj,
                    "cnpj": cnpj,
                    "data_limite_processamento": data_limite,
                    "DATA_LIMITE_PROCESSAMENTO": data_limite,
                },
            )
            log(f"Executando {sql_file.name} -> {output_path.name}")
            linhas = fetch_query_to_parquet(conn, sql, binds, output_path, fetch_size=fetch_size, source_name=sql_file.stem.upper())
            log(f"Concluído {sql_file.name}: {linhas:,} linhas")
            results.append(ExtractionResult(sql_file.stem.lower(), sql_file, output_path, linhas))
    finally:
        conn.close()
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Executa consultas Oracle por CNPJ, salva cada consulta em Parquet com o conteúdo exato retornado pelo Oracle "
            "e gera apenas as tabelas finais do fluxo de consolidação dentro da pasta produtos."
        )
    )
    parser.add_argument("--cnpj", required=True, help="Único dado de entrada de negócio usado nas consultas.")
    parser.add_argument("--data-limite", default=None, help="Data limite de processamento EFD no formato DD/MM/YYYY. O padrão é a data atual.")
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Diretório com os arquivos .sql. Padrão: mesmo diretório do script.",
    )
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("saida_produtos"),
        help="Diretório raiz onde será criada a pasta do CNPJ. Padrão: ./saida_produtos",
    )
    parser.add_argument("--fetch-size", type=int, default=50_000, help="Quantidade de linhas por lote.")
    parser.add_argument("--db-user", default=None, help="Usuário Oracle. Se omitido, usa DB_USER do .env")
    parser.add_argument("--db-password", default=None, help="Senha Oracle. Se omitido, usa DB_PASSWORD do .env")
    parser.add_argument("--extrair-apenas", action="store_true", help="Executa apenas a extração Oracle -> Parquet")
    parser.add_argument(
        "--consolidar-apenas",
        action="store_true",
        help="Executa apenas a geração das tabelas finais a partir dos parquets já existentes.",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        cnpj = sanitize_cnpj(args.cnpj)
        pasta_cnpj = args.saida / cnpj
        pasta_cnpj.mkdir(parents=True, exist_ok=True)
        (pasta_cnpj / "produtos").mkdir(parents=True, exist_ok=True)

        if not args.consolidar_apenas:
            extrair_consultas(
                sql_dir=args.sql_dir,
                pasta_cnpj=pasta_cnpj,
                cnpj=cnpj,
                fetch_size=args.fetch_size,
                usuario=args.db_user,
                senha=args.db_password,
                data_limite=args.data_limite,
            )

        if not args.extrair_apenas:
            materializar_tabelas_consolidacao(pasta_cnpj=pasta_cnpj, cnpj=cnpj)
            log("Tabelas finais da pasta produtos geradas com sucesso.")

        log(f"Saída concluída em {pasta_cnpj}")
        return 0
    except RuntimeError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERRO INESPERADO: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
