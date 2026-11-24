from pipeline import schema, sql_parse


def test_every_table_has_sql_select_in_schema_order():
    for key, t in schema.TABLES.items():
        sql = sql_parse.select_sql(key, "mp2.envelopes")
        assert f"AS {t.columns[-1]}" in sql and "FROM mp2.envelopes" in sql


def test_generated_ddl_matches_committed_file():
    from pathlib import Path
    committed = (Path(__file__).resolve().parents[1] / "clickhouse" / "ddl" / "005_envelopes_mv.sql").read_text()
    assert committed == sql_parse.generate_ddl("mp2"), "clickhouse/ddl/005_envelopes_mv.sql 을 다시 생성하세요: python -m pipeline.sql_parse > clickhouse/ddl/005_envelopes_mv.sql"
