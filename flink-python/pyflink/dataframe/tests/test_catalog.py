################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest
from unittest.mock import patch

from py4j.protocol import Py4JJavaError

import pyflink.dataframe as pf
from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import Catalog
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase
from pyflink.util.exceptions import CatalogException


class CatalogTests(PyFlinkDataFrameUTTestCase):
    def test_create_catalog_builds_descriptor(self):
        with patch.object(
            self.t_env, "create_catalog", wraps=self.t_env.create_catalog
        ) as create_catalog:
            pf.create_catalog(
                "my_catalog",
                {"type": "generic_in_memory", "default-database": "my_database"},
            )

        name, descriptor = create_catalog.call_args.args
        self.assertEqual(name, "my_catalog")
        options = descriptor._j_catalog_descriptor.getConfiguration().toMap()
        self.assertEqual(
            dict(options),
            {"type": "generic_in_memory", "default-database": "my_database"},
        )
        self.assertIn("my_catalog", pf.list_catalogs())
        self.assertEqual(
            pf.get_catalog("my_catalog").get_default_database(), "my_database"
        )

    def test_create_catalog_rejects_invalid_arguments(self):
        cases = [
            (None, {"type": "generic_in_memory"}, TypeError, "name must be a string"),
            ("", {"type": "generic_in_memory"}, ValueError, "name must not be empty"),
            ("my_catalog", ["type"], TypeError, "options must be a dict"),
            ("my_catalog", {1: "x"}, TypeError, "option keys must be strings"),
            ("my_catalog", {"": "x"}, ValueError, "option keys must not be empty"),
            (
                "my_catalog",
                {"type": "generic_in_memory", "flag": True},
                TypeError,
                "option 'flag' must have a string value",
            ),
        ]

        for name, options, error_type, message in cases:
            with self.subTest(name=name, options=options):
                with self.assertRaisesRegex(error_type, message):
                    pf.create_catalog(name, options)

        self.assertNotIn("my_catalog", pf.list_catalogs())

    def test_create_catalog_rejects_existing_catalog(self):
        with self.assertRaisesRegex(CatalogException, "Catalog default_catalog already exists"):
            pf.create_catalog("default_catalog", {"type": "generic_in_memory"})

    def test_create_catalog_translates_flink_errors(self):
        with self.assertRaises(ValueError) as context:
            pf.create_catalog("my_catalog", {"type": "no_such_catalog_type"})
        self.assertIn("Unable to create catalog 'my_catalog'", str(context.exception))
        self.assertIn("'type'='no_such_catalog_type'", str(context.exception))
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)
        self.assertNotIn("my_catalog", pf.list_catalogs())

    def test_get_catalog(self):
        self.assertIsNone(pf.get_catalog("missing"))

        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        catalog = pf.get_catalog("my_catalog")
        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(catalog.list_databases(), ["default"])

    def test_catalog_navigation(self):
        self.assertEqual(pf.get_current_catalog(), "default_catalog")
        self.assertEqual(pf.get_current_database(), "default_database")
        self.assertEqual(pf.list_catalogs(), ["default_catalog"])
        self.assertEqual(pf.list_databases(), ["default_database"])

        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        self.assertEqual(pf.list_catalogs(), ["default_catalog", "my_catalog"])

        pf.use_catalog("my_catalog")
        self.assertEqual(pf.get_current_catalog(), "my_catalog")
        self.assertEqual(pf.get_current_database(), "default")
        self.assertEqual(pf.list_databases(), ["default"])

        self.t_env.execute_sql("CREATE DATABASE my_database")
        self.assertEqual(sorted(pf.list_databases()), ["default", "my_database"])

        pf.use_database("my_database")
        self.assertEqual(pf.get_current_database(), "my_database")

        pf.use_catalog("default_catalog")
        self.assertEqual(pf.get_current_catalog(), "default_catalog")
        self.assertEqual(pf.get_current_database(), "default_database")

    def test_navigation_rejects_unknown_names(self):
        with self.assertRaisesRegex(
            CatalogException, r"A catalog with name \[missing\] does not exist"
        ):
            pf.use_catalog("missing")

        with self.assertRaisesRegex(
            CatalogException,
            r"A database with name \[missing\] does not exist in the catalog: \[default_catalog\]",
        ):
            pf.use_database("missing")

        self.assertEqual(pf.get_current_catalog(), "default_catalog")
        self.assertEqual(pf.get_current_database(), "default_database")

    def test_navigation_without_current_catalog(self):
        self.t_env.use_catalog(None)
        self.assertIsNone(pf.get_current_catalog())
        self.assertIsNone(pf.get_current_database())

        with self.assertRaisesRegex(ValueError, "A current catalog has not been set"):
            pf.list_databases()
        self.assertEqual(pf.get_catalog("default_catalog").list_databases(), ["default_database"])

        pf.use_catalog("default_catalog")
        self.assertEqual(pf.list_databases(), ["default_database"])

    def test_unexpected_errors_are_not_translated(self):
        gateway = get_gateway()
        j_error = gateway.jvm.org.apache.flink.table.api.TableException("boom")
        java_error = Py4JJavaError("An error occurred while calling createCatalog.", j_error)
        catalog_error = CatalogException("boom")
        python_error = RuntimeError("boom")

        for error in [java_error, catalog_error, python_error]:
            with self.subTest(error=type(error).__name__):
                with patch.object(self.t_env, "create_catalog", side_effect=error):
                    with self.assertRaises(type(error)) as context:
                        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
                self.assertIs(context.exception, error)

    def test_name_validation(self):
        for function in [pf.get_catalog, pf.use_catalog, pf.use_database]:
            with self.subTest(function=function.__name__):
                with self.assertRaisesRegex(TypeError, "name must be a string"):
                    function(None)
                with self.assertRaisesRegex(ValueError, "name must not be empty"):
                    function("")

    def test_uses_dataframe_table_environment(self):
        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        self.assertIn("my_catalog", self.t_env.list_catalogs())

        pf.use_catalog("my_catalog")
        self.assertEqual(self.t_env.get_current_catalog(), "my_catalog")


if __name__ == "__main__":
    unittest.main()
