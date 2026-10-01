import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from helpers.sql_queries import build_crud_query


class CrudQueryTests(unittest.TestCase):
    def test_get_and_delete_bind_the_key_for_both_databases(self):
        injected_key = "1' OR '1'='1"
        for db_type, placeholder, table, column in (
            ("postgres", "%s", '"users"', '"id"'),
            ("sqlserver", "?", "[users]", "[id]"),
        ):
            with self.subTest(db_type=db_type):
                for method, verb in (("get", "SELECT * FROM"), ("delete", "DELETE FROM")):
                    query, params = build_crud_query(
                        db_type, method, "users", "id", injected_key
                    )
                    self.assertEqual(query, f"{verb} {table} WHERE {column} = {placeholder}")
                    self.assertEqual(params, (injected_key,))
                    self.assertNotIn(injected_key, query)

    def test_insert_and_update_bind_every_value(self):
        record = [
            {"key": "name", "value": "O'Brien'}, balance=100 --"},
            {"key": "balance", "value": None},
        ]
        for db_type, placeholder, table, name, balance, key in (
            ("postgres", "%s", '"users"', '"name"', '"balance"', '"id"'),
            ("sqlserver", "?", "[users]", "[name]", "[balance]", "[id]"),
        ):
            with self.subTest(db_type=db_type):
                query, params = build_crud_query(db_type, "post", "users", "id", "NEW", record)
                self.assertEqual(
                    query,
                    f"INSERT INTO {table} ({name}, {balance}) VALUES ({placeholder}, {placeholder})",
                )
                self.assertEqual(params, (record[0]["value"], None))

                query, params = build_crud_query(
                    db_type, "post", "users", "id", "1 OR 1=1", record
                )
                self.assertEqual(
                    query,
                    f"UPDATE {table} SET {name} = {placeholder}, {balance} = {placeholder} "
                    f"WHERE {key} = {placeholder}",
                )
                self.assertEqual(params, (record[0]["value"], None, "1 OR 1=1"))

    def test_identifier_delimiters_are_escaped(self):
        self.assertEqual(
            build_crud_query("postgres", "get", 'us"ers', 'i"d', "1"),
            ('SELECT * FROM "us""ers" WHERE "i""d" = %s', ("1",)),
        )
        self.assertEqual(
            build_crud_query("sqlserver", "get", "us]ers", "i]d", "1"),
            ("SELECT * FROM [us]]ers] WHERE [i]]d] = ?", ("1",)),
        )

    def test_invalid_identifiers_and_empty_updates_are_rejected(self):
        with self.assertRaises(ValueError):
            build_crud_query("postgres", "get", "users\0; DROP TABLE users", "id", "1")
        with self.assertRaises(ValueError):
            build_crud_query("sqlserver", "post", "users", "id", "NEW", [])


if __name__ == "__main__":
    unittest.main()
