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

from pyflink.java_gateway import get_gateway
from pyflink.table import Schema, TableSchema, DataTypes

from pyflink.table.catalog import ObjectPath, Catalog, CatalogDatabase, CatalogBaseTable, \
    CatalogFunction, CatalogPartition, CatalogPartitionSpec, CatalogModel
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.util.exceptions import DatabaseNotExistException, FunctionNotExistException, \
    PartitionNotExistException, TableNotExistException, DatabaseAlreadyExistException, \
    FunctionAlreadyExistException, PartitionAlreadyExistsException, PartitionSpecInvalidException, \
    TableNotPartitionedException, TableAlreadyExistException, DatabaseNotEmptyException, \
    ModelNotExistException, ModelAlreadyExistException


class CatalogTestBase(PyFlinkTestCase):

    db1 = "db1"
    db2 = "db2"
    non_exist_database = "non-exist-db"
    t1 = "t1"
    t2 = "t2"
    t3 = "t3"
    m1 = "m1"
    m2 = "m2"
    test_catalog_name = "test-catalog"
    test_comment = "test comment"

    def setUp(self):
        super(CatalogTestBase, self).setUp()
        gateway = get_gateway()
        self.catalog = Catalog(gateway.jvm.GenericInMemoryCatalog(self.test_catalog_name))

        self.path1 = ObjectPath(self.db1, self.t1)
        self.path2 = ObjectPath(self.db2, self.t2)
        self.path3 = ObjectPath(self.db1, self.t2)
        self.path4 = ObjectPath(self.db1, self.t3)
        self.modelPath1 = ObjectPath(self.db1, self.m1)
        self.modelPath2 = ObjectPath(self.db1, self.m2)
        self.non_exist_db_path = ObjectPath.from_string("non.exist")
        self.non_exist_object_path = ObjectPath.from_string("db1.nonexist")

    def check_catalog_database_equals(self, cd1, cd2):
        self.assertEqual(cd1.get_comment(), cd2.get_comment())
        self.assertEqual(cd1.get_properties(), cd2.get_properties())

    def check_catalog_table_equals(self, t1, t2):
        self.assertEqual(t1.get_options(), t2.get_options())
        self.assertEqual(t1.get_comment(), t2.get_comment())

    def check_catalog_model_equals(self, m1, m2):
        self.assertEqual(m1.get_options(), m2.get_options())
        self.assertEqual(m1.get_comment(), m2.get_comment())

    def check_catalog_view_equals(self, v1, v2):
        self.assertEqual(v1.get_options(), v2.get_options())
        self.assertEqual(v1.get_comment(), v2.get_comment())

    def check_catalog_function_equals(self, f1, f2):
        self.assertEqual(f1.get_class_name(), f2.get_class_name())
        self.assertEqual(f1.is_generic(), f2.is_generic())
        self.assertEqual(f1.get_function_language(), f2.get_function_language())

    def check_catalog_partition_equals(self, p1, p2):
        self.assertEqual(p1.get_properties(), p2.get_properties())

    @staticmethod
    def create_db():
        return CatalogDatabase.create_instance({"k1": "v1"}, CatalogTestBase.test_comment)

    @staticmethod
    def create_another_db():
        return CatalogDatabase.create_instance({"k2": "v2"}, "this is another database.")

    @staticmethod
    def create_table_schema():
        return TableSchema(["first", "second", "third"],
                           [DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()])

    @staticmethod
    def create_model_schema():
        return Schema.new_builder().column("id", DataTypes.STRING()).build()

    @staticmethod
    def create_another_table_schema():
        return TableSchema(["first2", "second", "third"],
                           [DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()])

    @staticmethod
    def get_batch_table_properties():
        return {"is_streaming": "false"}

    @staticmethod
    def get_streaming_table_properties():
        return {"is_streaming": "true"}

    @staticmethod
    def create_partition_keys():
        return ["second", "third"]

    @staticmethod
    def create_model():
        return CatalogModel.create_model(
            CatalogTestBase.create_model_schema(),
            CatalogTestBase.create_model_schema(),
            options={},
            comment="some comment")

    @staticmethod
    def create_another_model():
        return CatalogModel.create_model(
            CatalogTestBase.create_model_schema(),
            CatalogTestBase.create_model_schema(),
            options={"key": "value"},
            comment="some comment")

    @staticmethod
    def create_table():
        return CatalogBaseTable.create_table(
            schema=CatalogTestBase.create_table_schema(),
            properties=CatalogTestBase.get_batch_table_properties(),
            comment=CatalogTestBase.test_comment)

    @staticmethod
    def create_another_table():
        return CatalogBaseTable.create_table(
            schema=CatalogTestBase.create_another_table_schema(),
            properties=CatalogTestBase.get_batch_table_properties(),
            comment=CatalogTestBase.test_comment)

    @staticmethod
    def create_stream_table():
        return CatalogBaseTable.create_table(
            schema=CatalogTestBase.create_table_schema(),
            properties=CatalogTestBase.get_streaming_table_properties(),
            comment=CatalogTestBase.test_comment)

    @staticmethod
    def create_partitioned_table():
        return CatalogBaseTable.create_table(
            schema=CatalogTestBase.create_table_schema(),
            partition_keys=CatalogTestBase.create_partition_keys(),
            properties=CatalogTestBase.get_batch_table_properties(),
            comment=CatalogTestBase.test_comment)

    @staticmethod
    def create_another_partitioned_table():
        return CatalogBaseTable.create_table(
            schema=CatalogTestBase.create_another_table_schema(),
            partition_keys=CatalogTestBase.create_partition_keys(),
            properties=CatalogTestBase.get_batch_table_properties(),
            comment=CatalogTestBase.test_comment)

    @staticmethod
    def create_view():
        table_schema = CatalogTestBase.create_table_schema()
        return CatalogBaseTable.create_view(
            "select * from t1",
            "select * from test-catalog.db1.t1",
            table_schema,
            {},
            "This is a view")

    @staticmethod
    def create_another_view():
        table_schema = CatalogTestBase.create_another_table_schema()
        return CatalogBaseTable.create_view(
            "select * from t2",
            "select * from test-catalog.db2.t2",
            table_schema,
            {},
            "This is another view")

    @staticmethod
    def create_function():
        return CatalogFunction.create_instance(
            "org.apache.flink.table.functions.python.PythonScalarFunction", "Java")

    @staticmethod
    def create_another_function():
        return CatalogFunction.create_instance(
            "org.apache.flink.table.functions.ScalarFunction", "Java")

    @staticmethod
    def create_partition_spec():
        return CatalogPartitionSpec({"third": "2000", "second": "bob"})

    @staticmethod
    def create_another_partition_spec():
        return CatalogPartitionSpec({"third": "2010", "second": "bob"})

    @staticmethod
    def create_partition():
        return CatalogPartition.create_instance(
            CatalogTestBase.get_batch_table_properties(), "catalog partition tests")

    @staticmethod
    def create_partition_spec_subset():
        return CatalogPartitionSpec({"second": "bob"})

    @staticmethod
    def create_another_partition_spec_subset():
        return CatalogPartitionSpec({"third": "2000"})

    @staticmethod
    def create_invalid_partition_spec_subset():
        return CatalogPartitionSpec({"third": "2010"})

    def test_create_db(self):
        self.assertFalse(self.catalog.database_exists(self.db1))
        catalog_db = self.create_db()
        self.catalog.create_database(self.db1, catalog_db, False)

        self.assertTrue(self.catalog.database_exists(self.db1))
        self.check_catalog_database_equals(catalog_db, self.catalog.get_database(self.db1))

    def test_create_db_database_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(DatabaseAlreadyExistException):
            self.catalog.create_database(self.db1, self.create_db(), False)

    def test_create_db_database_already_exist_ignored(self):
        catalog_db = self.create_db()
        self.catalog.create_database(self.db1, catalog_db, False)
        dbs = self.catalog.list_databases()

        self.check_catalog_database_equals(catalog_db, self.catalog.get_database(self.db1))
        self.assertEqual(2, len(dbs))
        self.assertEqual({self.db1, self.catalog.get_default_database()}, set(dbs))

        self.catalog.create_database(self.db1, self.create_another_db(), True)

        self.check_catalog_database_equals(catalog_db, self.catalog.get_database(self.db1))
        self.assertEqual(2, len(dbs))
        self.assertEqual({self.db1, self.catalog.get_default_database()}, set(dbs))

    def test_get_db_database_not_exist_exception(self):
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.get_database("nonexistent")

    def test_drop_db(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertTrue(self.catalog.database_exists(self.db1))

        self.catalog.drop_database(self.db1, False)

        self.assertFalse(self.catalog.database_exists(self.db1))

    def test_drop_db_database_not_exist_exception(self):
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.drop_database(self.db1, False)

    def test_drop_db_database_not_exist_ignore(self):
        self.catalog.drop_database(self.db1, True)

    def test_drop_db_database_not_empty_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        with self.assertRaises(DatabaseNotEmptyException):
            self.catalog.drop_database(self.db1, True)

    def test_alter_db(self):
        db = self.create_db()
        self.catalog.create_database(self.db1, db, False)

        new_db = self.create_another_db()
        self.catalog.alter_database(self.db1, new_db, False)

        new_properties = self.catalog.get_database(self.db1).get_properties()
        old_properties = db.get_properties()
        self.assertFalse(all(k in new_properties for k in old_properties.keys()))
        self.check_catalog_database_equals(new_db, self.catalog.get_database(self.db1))

    def test_alter_db_database_not_exist_exception(self):
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.alter_database("nonexistent", self.create_db(), False)

    def test_alter_db_database_not_exist_ignored(self):
        self.catalog.alter_database("nonexistent", self.create_db(), True)
        self.assertFalse(self.catalog.database_exists("nonexistent"))

    def test_db_exists(self):
        self.assertFalse(self.catalog.database_exists("nonexistent"))
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.assertTrue(self.catalog.database_exists(self.db1))

    def test_create_table_streaming(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_stream_table()
        self.catalog.create_table(self.path1, table, False)
        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

    def test_create_table_batch(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        # Non - partitioned table
        table = self.create_table()
        self.catalog.create_table(self.path1, table, False)

        table_created = self.catalog.get_table(self.path1)

        self.check_catalog_table_equals(table, table_created)
        self.assertEqual(self.test_comment, table_created.get_description())

        tables = self.catalog.list_tables(self.db1)

        self.assertEqual(1, len(tables))
        self.assertEqual(self.path1.get_object_name(), tables[0])

        self.catalog.drop_table(self.path1, False)

        # Partitioned table
        self.table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

        tables = self.catalog.list_tables(self.db1)

        self.assertEqual(1, len(tables))
        self.assertEqual(self.path1.get_object_name(), tables[0])

    def test_create_table_database_not_exist_exception(self):
        self.assertFalse(self.catalog.database_exists(self.db1))

        with self.assertRaises(DatabaseNotExistException):
            self.catalog.create_table(self.non_exist_object_path, self.create_table(), False)

    def test_create_table_table_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        with self.assertRaises(TableAlreadyExistException):
            self.catalog.create_table(self.path1, self.create_table(), False)

    def test_create_table_table_already_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        table = self.create_table()
        self.catalog.create_table(self.path1, table, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

        self.catalog.create_table(self.path1, self.create_another_table(), True)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

    def test_get_table_table_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(TableNotExistException):
            self.catalog.get_table(self.non_exist_object_path)

    def test_get_table_table_not_exist_exception_no_db(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.get_table(self.non_exist_object_path)

    def test_drop_table_non_partitioned_table(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        self.assertTrue(self.catalog.table_exists(self.path1))

        self.catalog.drop_table(self.path1, False)

        self.assertFalse(self.catalog.table_exists(self.path1))

    def test_drop_table_table_not_exist_exception(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.drop_table(self.non_exist_db_path, False)

    def test_drop_table_table_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.drop_table(self.non_exist_object_path, True)

    def test_alter_table(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        # Non - partitioned table
        table = self.create_table()
        self.catalog.create_table(self.path1, table, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

        new_table = self.create_another_table()
        self.catalog.alter_table(self.path1, new_table, False)

        self.assertNotEqual(table, self.catalog.get_table(self.path1))
        self.check_catalog_table_equals(new_table, self.catalog.get_table(self.path1))

        self.catalog.drop_table(self.path1, False)

        # Partitioned table
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

        new_table = self.create_another_partitioned_table()
        self.catalog.alter_table(self.path1, new_table, False)

        self.check_catalog_table_equals(new_table, self.catalog.get_table(self.path1))

        # View
        view = self.create_view()
        self.catalog.create_table(self.path3, view, False)

        self.check_catalog_view_equals(view, self.catalog.get_table(self.path3))

        new_view = self.create_another_view()
        self.catalog.alter_table(self.path3, new_view, False)

        self.assertNotEqual(view, self.catalog.get_table(self.path3))
        self.check_catalog_view_equals(new_view, self.catalog.get_table(self.path3))

    def test_alter_table_table_not_exist_exception(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.alter_table(self.non_exist_db_path, self.create_table(), False)

    def test_alter_table_table_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.alter_table(self.non_exist_object_path, self.create_table(), True)

        self.assertFalse(self.catalog.table_exists(self.non_exist_object_path))

    def test_rename_table_non_partitioned_table(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_table()
        self.catalog.create_table(self.path1, table, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path1))

        self.catalog.rename_table(self.path1, self.t2, False)

        self.check_catalog_table_equals(table, self.catalog.get_table(self.path3))
        self.assertFalse(self.catalog.table_exists(self.path1))

    def test_rename_table_table_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(TableNotExistException):
            self.catalog.rename_table(self.path1, self.t2, False)

    def test_rename_table_table_not_exist_exception_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.rename_table(self.path1, self.t2, True)

    def test_rename_table_table_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_table()
        self.catalog.create_table(self.path1, table, False)
        self.catalog.create_table(self.path3, self.create_another_table(), False)

        with self.assertRaises(TableAlreadyExistException):
            self.catalog.rename_table(self.path1, self.t2, False)

    def test_list_tables(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.catalog.create_table(self.path1, self.create_table(), False)
        self.catalog.create_table(self.path3, self.create_table(), False)
        self.catalog.create_table(self.path4, self.create_view(), False)

        self.assertEqual(3, len(self.catalog.list_tables(self.db1)))
        self.assertEqual(1, len(self.catalog.list_views(self.db1)))

    def test_table_exists(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertFalse(self.catalog.table_exists(self.path1))

        self.catalog.create_table(self.path1, self.create_table(), False)

        self.assertTrue(self.catalog.table_exists(self.path1))

    def test_create_model_database_not_exist_exception(self):
        self.assertFalse(self.catalog.database_exists(self.db1))

        with self.assertRaises(DatabaseNotExistException):
            self.catalog.create_model(self.non_exist_object_path, self.create_model(), False)

    def test_create_model_model_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_model(self.modelPath1, self.create_model(), False)

        with self.assertRaises(ModelAlreadyExistException):
            self.catalog.create_model(self.modelPath1, self.create_model(), False)

    def test_create_model_model_already_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        model = self.create_model()
        self.catalog.create_model(self.modelPath1, model, False)

        self.check_catalog_model_equals(model, self.catalog.get_model(self.modelPath1))

        self.catalog.create_model(self.modelPath1, self.create_another_model(), True)

        self.check_catalog_model_equals(model, self.catalog.get_model(self.modelPath1))

    def test_get_model_model_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(ModelNotExistException):
            self.catalog.get_model(self.non_exist_object_path)

    def test_get_model_model_not_exist_exception_no_db(self):
        with self.assertRaises(ModelNotExistException):
            self.catalog.get_model(self.non_exist_object_path)

    def test_drop_model_model_not_exist_exception(self):
        with self.assertRaises(ModelNotExistException):
            self.catalog.drop_model(self.non_exist_db_path, False)

    def test_drop_model_model_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.drop_model(self.non_exist_object_path, True)

    def test_alter_model(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        model = self.create_model()
        self.catalog.create_model(self.modelPath1, model, False)

        self.check_catalog_model_equals(model, self.catalog.get_model(self.modelPath1))

        new_model = self.create_another_model()
        self.catalog.alter_model(self.modelPath1, new_model, False)

        self.assertNotEqual(model, self.catalog.get_model(self.modelPath1))
        self.check_catalog_model_equals(new_model, self.catalog.get_model(self.modelPath1))
        self.catalog.drop_model(self.modelPath1, False)

    def test_alter_model_model_not_exist_exception(self):
        with self.assertRaises(ModelNotExistException):
            self.catalog.alter_model(self.non_exist_db_path, self.create_model(), False)

    def test_alter_model_model_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.alter_model(self.non_exist_object_path, self.create_model(), True)

        self.assertFalse(self.catalog.table_exists(self.non_exist_object_path))

    def test_rename_model(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        model = self.create_model()
        self.catalog.create_model(self.modelPath1, model, False)

        self.check_catalog_model_equals(model, self.catalog.get_model(self.modelPath1))

        self.catalog.rename_model(self.modelPath1, self.m2, False)

        self.check_catalog_model_equals(model, self.catalog.get_model(self.modelPath2))
        self.assertFalse(self.catalog.model_exists(self.modelPath1))

    def test_rename_model_model_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(ModelNotExistException):
            self.catalog.rename_model(self.modelPath1, self.m2, False)

    def test_rename_model_model_not_exist_exception_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.rename_model(self.modelPath1, self.m2, True)

    def test_rename_model_model_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        model = self.create_model()
        self.catalog.create_model(self.modelPath1, model, False)
        self.catalog.create_model(self.modelPath2, self.create_another_model(), False)

        with self.assertRaises(ModelAlreadyExistException):
            self.catalog.rename_model(self.modelPath1, self.m2, False)

    def test_list_models(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.catalog.create_model(self.modelPath1, self.create_model(), False)
        self.catalog.create_model(self.modelPath2, self.create_model(), False)

        self.assertEqual(2, len(self.catalog.list_models(self.db1)))

    def test_model_exists(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertFalse(self.catalog.model_exists(self.modelPath1))

        self.catalog.create_model(self.modelPath1, self.create_model(), False)

        self.assertTrue(self.catalog.model_exists(self.modelPath1))

    def test_create_view(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertFalse(self.catalog.table_exists(self.path1))

        view = self.create_view()
        self.catalog.create_table(self.path1, view, False)

        self.check_catalog_view_equals(view, self.catalog.get_table(self.path1))

    def test_create_view_database_not_exist_exception(self):
        self.assertFalse(self.catalog.database_exists(self.db1))

        with self.assertRaises(DatabaseNotExistException):
            self.catalog.create_table(self.non_exist_object_path, self.create_view(), False)

    def test_create_view_table_already_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_view(), False)

        with self.assertRaises(TableAlreadyExistException):
            self.catalog.create_table(self.path1, self.create_view(), False)

    def test_create_view_table_already_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        view = self.create_view()
        self.catalog.create_table(self.path1, view, False)

        self.check_catalog_view_equals(view, self.catalog.get_table(self.path1))

        self.catalog.create_table(self.path1, self.create_another_view(), True)

        self.check_catalog_view_equals(view, self.catalog.get_table(self.path1))

    def test_drop_view(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_view(), False)

        self.assertTrue(self.catalog.table_exists(self.path1))

        self.catalog.drop_table(self.path1, False)

        self.assertFalse(self.catalog.table_exists(self.path1))

    def test_alter_view(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        view = self.create_view()
        self.catalog.create_table(self.path1, view, False)

        self.check_catalog_view_equals(view, self.catalog.get_table(self.path1))

        new_view = self.create_another_view()
        self.catalog.alter_table(self.path1, new_view, False)

        self.check_catalog_view_equals(new_view, self.catalog.get_table(self.path1))

    def test_alter_view_table_not_exist_exception(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.alter_table(self.non_exist_db_path, self.create_table(), False)

    def test_alter_view_table_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.alter_table(self.non_exist_object_path, self.create_view(), True)

        self.assertFalse(self.catalog.table_exists(self.non_exist_object_path))

    def test_list_view(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertTrue(0 == len(self.catalog.list_tables(self.db1)))

        self.catalog.create_table(self.path1, self.create_view(), False)
        self.catalog.create_table(self.path3, self.create_table(), False)

        self.assertEqual(2, len(self.catalog.list_tables(self.db1)))
        self.assertEqual({self.path1.get_object_name(), self.path3.get_object_name()},
                         set(self.catalog.list_tables(self.db1)))
        self.assertEqual([self.path1.get_object_name()], self.catalog.list_views(self.db1))

    def test_rename_view(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_view(), False)

        self.assertTrue(self.catalog.table_exists(self.path1))

        self.catalog.rename_table(self.path1, self.t2, False)

        self.assertFalse(self.catalog.table_exists(self.path1))
        self.assertTrue(self.catalog.table_exists(self.path3))

    def test_create_function(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        self.assertFalse(self.catalog.function_exists(self.path1))

        self.catalog.create_function(self.path1, self.create_function(), False)

        self.assertTrue(self.catalog.function_exists(self.path1))

    def test_create_function_database_not_exist_exception(self):
        self.assertFalse(self.catalog.database_exists(self.db1))

        with self.assertRaises(DatabaseNotExistException):
            self.catalog.create_function(self.path1, self.create_function(), False)

    def test_create_functin_function_already_exist_function(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_function(self.path1, self.create_function(), False)

        self.assertTrue(self.catalog.function_exists(self.path1))

        # test 'ignoreIfExist' flag
        self.catalog.create_function(self.path1, self.create_another_function(), True)

        with self.assertRaises(FunctionAlreadyExistException):
            self.catalog.create_function(self.path1, self.create_function(), False)

    def test_alter_function(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        func = self.create_function()
        self.catalog.create_function(self.path1, func, False)

        self.check_catalog_function_equals(func, self.catalog.get_function(self.path1))

        new_func = self.create_another_function()
        self.catalog.alter_function(self.path1, new_func, False)
        actual = self.catalog.get_function(self.path1)

        self.assertFalse(func.get_class_name() == (actual.get_class_name()))
        self.check_catalog_function_equals(new_func, actual)

    def test_alter_function_function_not_exist_exception(self):
        with self.assertRaises(FunctionNotExistException):
            self.catalog.alter_function(self.non_exist_object_path, self.create_function(), False)

    def test_alter_function_function_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.alter_function(self.non_exist_object_path, self.create_function(), True)

        self.assertFalse(self.catalog.function_exists(self.non_exist_object_path))

    def test_list_functions(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        func = self.create_function()
        self.catalog.create_function(self.path1, func, False)

        self.assertEqual(self.path1.get_object_name(), self.catalog.list_functions(self.db1)[0])

    def test_list_functions_database_not_exist_exception(self):
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.list_functions(self.db1)

    def test_get_function_function_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(FunctionNotExistException):
            self.catalog.get_function(self.non_exist_object_path)

    def test_get_function_function_not_exist_exception_no_db(self):
        with self.assertRaises(FunctionNotExistException):
            self.catalog.get_function(self.non_exist_object_path)

    def test_drop_function(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_function(self.path1, self.create_function(), False)

        self.assertTrue(self.catalog.function_exists(self.path1))

        self.catalog.drop_function(self.path1, False)

        self.assertFalse(self.catalog.function_exists(self.path1))

    def test_drop_function_function_not_exist_exception(self):
        with self.assertRaises(FunctionNotExistException):
            self.catalog.drop_function(self.non_exist_db_path, False)

    def test_drop_function_function_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.drop_function(self.non_exist_object_path, True)
        self.catalog.drop_database(self.db1, False)

    def test_create_partition(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)

        self.assertTrue(0 == len(self.catalog.list_partitions(self.path1)))

        self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                      self.create_partition(), False)

        self.check_catalog_partition_equals(self.create_partition(),
                                            self.catalog.get_partition(
                                                self.path1, self.create_partition_spec()))

        self.catalog.create_partition(
            self.path1, self.create_another_partition_spec(), self.create_partition(), False)

        self.check_catalog_partition_equals(self.create_partition(),
                                            self.catalog.get_partition(
                                                self.path1, self.create_another_partition_spec()))

    def test_create_partition_table_not_exist_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        with self.assertRaises(TableNotExistException):
            self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                          self.create_partition(), False)

    def test_create_partition_table_not_partitoned_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        with self.assertRaises(TableNotPartitionedException):
            self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                          self.create_partition(), False)

    def test_create_partition_partition_spec_invalid_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        partition_spec = self.create_invalid_partition_spec_subset()

        with self.assertRaises(PartitionSpecInvalidException):
            self.catalog.create_partition(
                self.path1, partition_spec, self.create_partition(), False)

    def test_create_partition_partition_already_exists_exception(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        partition = self.create_partition()
        self.catalog.create_partition(self.path1, self.create_partition_spec(), partition, False)

        partition_spec = self.create_partition_spec()
        with self.assertRaises(PartitionAlreadyExistsException):
            self.catalog.create_partition(
                self.path1, partition_spec, self.create_partition(), False)

    def test_create_partition_partition_already_exists_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)

        partition_spec = self.create_partition_spec()
        self.catalog.create_partition(self.path1, partition_spec, self.create_partition(), False)
        self.catalog.create_partition(self.path1, partition_spec, self.create_partition(), True)

    def test_drop_partition(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                      self.create_partition(), False)

        self.catalog.drop_partition(self.path1, self.create_partition_spec(), False)

        self.assertEqual([], self.catalog.list_partitions(self.path1))

    def test_drop_partition_table_not_exist(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.drop_partition(self.path1, partition_spec, False)

    def test_drop_partition_table_not_partitioned(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.drop_partition(self.path1, partition_spec, False)

    def test_drop_partition_partition_spec_invalid(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        partition_spec = self.create_invalid_partition_spec_subset()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.drop_partition(self.path1, partition_spec, False)

    def test_drop_partition_patition_not_exist(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.drop_partition(self.path1, partition_spec, False)

    def test_drop_partition_patition_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.drop_partition(self.path1, self.create_partition_spec(), True)

    def test_alter_partition(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                      self.create_partition(), False)

        cp = self.catalog.get_partition(self.path1, self.create_partition_spec())
        self.check_catalog_partition_equals(self.create_partition(), cp)
        self.assertIsNone(cp.get_properties().get("k"))

        another = CatalogPartition.create_instance(
            {"is_streaming": "false", "k": "v"}, "catalog partition")
        self.catalog.alter_partition(self.path1, self.create_partition_spec(), another, False)

        cp = self.catalog.get_partition(self.path1, self.create_partition_spec())
        self.check_catalog_partition_equals(another, cp)
        self.assertEqual("v", cp.get_properties().get("k"))

    def test_alter_partition_table_not_exist(self):
        self.catalog.create_database(self.db1, self.create_db(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.alter_partition(self.path1, partition_spec, self.create_partition(), False)

    def test_alter_partition_table_not_partitioned(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.alter_partition(self.path1, partition_spec, self.create_partition(), False)

    def test_alter_partition_partition_spec_invalid(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        partition_spec = self.create_invalid_partition_spec_subset()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.alter_partition(self.path1, partition_spec, self.create_partition(), False)

    def test_alter_partition_partition_not_exist(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.alter_partition(self.path1, partition_spec, self.create_partition(), False)

    def test_alter_partition_partition_not_exist_ignored(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.alter_partition(self.path1, self.create_partition_spec(),
                                     self.create_partition(), True)

    def test_get_partition_table_not_exists(self):
        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.get_partition(self.path1, partition_spec)

    def test_get_partition_table_not_partitioned(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.get_partition(self.path1, partition_spec)

    def test_get_partition_partition_spec_invalid_invalid_partition_spec(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        partition_spec = self.create_invalid_partition_spec_subset()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.get_partition(self.path1, partition_spec)

    def test_get_partition_partition_spec_invalid_size_not_equal(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        table = self.create_partitioned_table()
        self.catalog.create_table(self.path1, table, False)

        partition_spec = self.create_partition_spec_subset()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.get_partition(self.path1, partition_spec)

    def test_get_partition_partition_not_exist(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)

        partition_spec = self.create_partition_spec()

        with self.assertRaises(PartitionNotExistException):
            self.catalog.get_partition(self.path1, partition_spec)

    def test_partition_exists(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                      self.create_partition(), False)

        self.assertTrue(self.catalog.partition_exists(self.path1, self.create_partition_spec()))
        self.assertFalse(self.catalog.partition_exists(self.path2, self.create_partition_spec()))
        self.assertFalse(self.catalog.partition_exists(ObjectPath.from_string("non.exist"),
                                                       self.create_partition_spec()))

    def test_list_partition_partial_spec(self):
        self.catalog.create_database(self.db1, self.create_db(), False)
        self.catalog.create_table(self.path1, self.create_partitioned_table(), False)
        self.catalog.create_partition(self.path1, self.create_partition_spec(),
                                      self.create_partition(), False)
        self.catalog.create_partition(self.path1, self.create_another_partition_spec(),
                                      self.create_partition(), False)

        self.assertEqual(2,
                         len(self.catalog.list_partitions(
                             self.path1, self.create_partition_spec_subset())))
        self.assertEqual(1,
                         len(self.catalog.list_partitions(
                             self.path1, self.create_another_partition_spec_subset())))
