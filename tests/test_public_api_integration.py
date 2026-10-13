from __future__ import annotations

import csv
import io
import socket
import zipfile
from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
import requests
import yaml

from ukam_os_builder import inspect_flatfile_variants, run_from_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")


@pytest.fixture(params=[False, True], ids=["no-credentials", "credentials-present"])
def offline_network(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("OS_PROJECT_API_KEY", "OS_PROJECT_API_SECRET"):
        if request.param:
            monkeypatch.setenv(name, "offline-test")
        else:
            monkeypatch.delenv(name, raising=False)

    def reject_network(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Local ABP processing must not attempt network access")

    monkeypatch.setattr(requests.sessions.Session, "request", reject_network)
    monkeypatch.setattr(socket.socket, "connect", reject_network)


def _write_local_abp_inputs(directory: Path, start_at: str) -> None:
    schema_path = (
        Path(__file__).parents[1] / "ukam_os_builder/data_sources/abp/schemas/abp_schema.yaml"
    )
    schema = yaml.safe_load(schema_path.read_text())
    records = {
        "street_descriptor": {
            "record_identifier": 15,
            "usrn": 1,
            "street_description": "TEST ROAD",
            "town_name": "LONDON",
            "language": "ENG",
        },
        "blpu": {
            "record_identifier": 21,
            "uprn": 1001,
            "logical_status": 1,
            "blpu_state": "2",
            "postcode_locator": "SW1A 1AA",
            "country": "E",
        },
        "lpi": {
            "record_identifier": 24,
            "uprn": 1001,
            "lpi_key": "local-lpi",
            "language": "ENG",
            "logical_status": 1,
            "pao_start_number": 12,
            "usrn": 1,
            "official_flag": "Y",
        },
        "delivery_point": {
            "record_identifier": 28,
            "uprn": 1001,
            "udprn": 4001,
            "building_number": 12,
            "thoroughfare": "POSTAL ROAD",
            "post_town": "LONDON",
            "postcode": "SW1A 1AA",
        },
        "organisation": {},
        "classification": {
            "record_identifier": 32,
            "uprn": 1001,
            "class_key": "local-class",
            "classification_code": "RD06",
            "class_scheme": "AddressBase Premium Classification Scheme",
        },
    }
    for record in records.values():
        if record:
            record.update(change_type="I", pro_order=1)
    directory.mkdir()
    if start_at == "flatfile":
        raw = directory / "raw"
        raw.mkdir()
        with duckdb.connect() as con:
            for table, record in records.items():
                columns = ", ".join(
                    f'"{name}" {column["type"]}'
                    for name, column in schema[table]["columns"].items()
                )
                con.execute(f"CREATE TABLE {table} ({columns})")
                if record:
                    placeholders = ", ".join("?" for _ in record)
                    con.execute(
                        f"INSERT INTO {table} ({', '.join(record)}) VALUES ({placeholders})",
                        list(record.values()),
                    )
                con.execute(
                    f"COPY {table} TO '{(raw / (table + '.parquet')).as_posix()}' (FORMAT PARQUET)"
                )
        return

    text = io.StringIO()
    writer = csv.writer(text)
    for table, record in records.items():
        if record:
            writer.writerow([record.get(column, "") for column in schema[table]["columns"]])
    if start_at == "extract":
        with zipfile.ZipFile(directory / "AddressBasePremium_local.zip", "w") as archive:
            archive.writestr("nested/local.csv", text.getvalue())
    else:
        (directory / "local.csv").write_text(text.getvalue(), encoding="utf-8")


@pytest.mark.parametrize("start_at", ["extract", "split", "flatfile"])
def test_abp_local_inputs_build_canonical_output_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    offline_network: None,
    start_at: str,
) -> None:
    inputs = tmp_path / "managed-input"
    _write_local_abp_inputs(inputs, start_at)
    original_inputs = {path: path.read_bytes() for path in inputs.rglob("*") if path.is_file()}
    input_setting = {
        "extract": "downloads_dir",
        "split": "extracted_dir",
        "flatfile": "parquet_dir",
    }[start_at]
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        yaml.safe_dump(
            {
                "source": {"type": "abp"},
                "paths": {"work_dir": "./work", "overrides": {input_setting: "./managed-input"}},
                "os_downloads": {"package_id": "local", "version_id": "local"},
                "processing": {"num_chunks": 1},
            }
        ),
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    steps = ["extract", "split", "flatfile"]
    for step in steps[steps.index(start_at) :]:
        settings = run_from_config(config_path, step=step, check_api=False)

    output = settings.paths.output_dir / "abp_for_uk_address_matcher.chunk_001_of_001.parquet"
    with duckdb.connect() as con:
        rows = con.execute(
            """
            SELECT unique_id, address_concat, postcode, source, classification_code, udprn
            FROM read_parquet(?) ORDER BY address_concat
            """,
            [str(output)],
        ).fetchall()
    assert rows == [
        (1001, "12 POSTAL ROAD LONDON", "SW1A 1AA", "DELIVERY_POINT", "RD06", 4001),
        (1001, "12 TEST ROAD LONDON", "SW1A 1AA", "LPI", "RD06", 4001),
    ]
    assert {
        path: path.read_bytes() for path in inputs.rglob("*") if path.is_file()
    } == original_inputs


def test_abp_presplit_missing_table_fails_offline(tmp_path: Path, offline_network: None) -> None:
    inputs = tmp_path / "managed-input"
    _write_local_abp_inputs(inputs, "flatfile")
    (inputs / "raw/classification.parquet").unlink()
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: abp
        paths:
          work_dir: ./work
          overrides:
            parquet_dir: ./managed-input
        os_downloads:
          package_id: local
          version_id: local
        """,
    )

    with pytest.raises(FileNotFoundError, match="classification"):
        run_from_config(config_path, step="flatfile", check_api=False)
    assert not list((tmp_path / "work/output").glob("*.parquet"))


def test_package_root_inspect_flatfile_variants(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (4001::BIGINT, 'A'::VARCHAR),
                    (4001::BIGINT, 'B'::VARCHAR),
                    (4002::BIGINT, 'C'::VARCHAR)
            ) AS t(unique_id, address_concat)
        ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)
        """
    )

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd
        paths:
          output_dir: ./out
        """,
    )

    result = inspect_flatfile_variants(config_path=config_path, show=False)
    assert result["selected_uprn"] == 4001
    assert result["variant_count"] == 2
