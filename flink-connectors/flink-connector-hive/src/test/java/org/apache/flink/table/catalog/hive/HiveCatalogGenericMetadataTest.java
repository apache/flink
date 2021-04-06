/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.TestGenericUDF;
import org.apache.flink.table.functions.TestSimpleUDF;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for HiveCatalog on generic metadata. */
public class HiveCatalogGenericMetadataTest extends HiveCatalogMetadataTestBase {

    @BeforeClass
    public static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    // ------ tables ------

    @Test
    public void testGenericTableSchema() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        TableSchema tableSchema =
                TableSchema.builder()
                        .fields(
                                new String[] {"col1", "col2", "col3"},
                                new DataType[] {
                                    DataTypes.TIMESTAMP(3),
                                    DataTypes.TIMESTAMP(6),
                                    DataTypes.TIMESTAMP(9)
                                })
                        .watermark("col3", "col3", DataTypes.TIMESTAMP(9))
                        .build();

        ObjectPath tablePath = new ObjectPath(db1, "generic_table");
        try {
            catalog.createTable(
                    tablePath,
                    new CatalogTableImpl(tableSchema, getBatchTableProperties(), TEST_COMMENT),
                    false);

            assertEquals(tableSchema, catalog.getTable(tablePath).getSchema());
        } finally {
            catalog.dropTable(tablePath, true);
        }
    }

    @Test
    // NOTE: Be careful to modify this test, it is important to backward compatibility
    public void testTableSchemaCompatibility() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        try {
            // table with numeric types
            ObjectPath tablePath = new ObjectPath(db1, "generic1");
            Table hiveTable =
                    org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                            tablePath.getDatabaseName(), tablePath.getObjectName());
            hiveTable.setDbName(tablePath.getDatabaseName());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().putAll(getBatchTableProperties());
            hiveTable.getParameters().put("flink.generic.table.schema.0.name", "ti");
            hiveTable.getParameters().put("flink.generic.table.schema.0.data-type", "TINYINT");
            hiveTable.getParameters().put("flink.generic.table.schema.1.name", "si");
            hiveTable.getParameters().put("flink.generic.table.schema.1.data-type", "SMALLINT");
            hiveTable.getParameters().put("flink.generic.table.schema.2.name", "i");
            hiveTable.getParameters().put("flink.generic.table.schema.2.data-type", "INT");
            hiveTable.getParameters().put("flink.generic.table.schema.3.name", "bi");
            hiveTable.getParameters().put("flink.generic.table.schema.3.data-type", "BIGINT");
            hiveTable.getParameters().put("flink.generic.table.schema.4.name", "f");
            hiveTable.getParameters().put("flink.generic.table.schema.4.data-type", "FLOAT");
            hiveTable.getParameters().put("flink.generic.table.schema.5.name", "d");
            hiveTable.getParameters().put("flink.generic.table.schema.5.data-type", "DOUBLE");
            hiveTable.getParameters().put("flink.generic.table.schema.6.name", "de");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.6.data-type", "DECIMAL(10, 5)");
            hiveTable.getParameters().put("flink.generic.table.schema.7.name", "cost");
            hiveTable.getParameters().put("flink.generic.table.schema.7.expr", "`d` * `bi`");
            hiveTable.getParameters().put("flink.generic.table.schema.7.data-type", "DOUBLE");
            ((HiveCatalog) catalog).client.createTable(hiveTable);
            CatalogBaseTable catalogBaseTable = catalog.getTable(tablePath);
            assertTrue(
                    Boolean.parseBoolean(
                            catalogBaseTable.getOptions().get(CatalogPropertiesUtil.IS_GENERIC)));
            TableSchema expectedSchema =
                    TableSchema.builder()
                            .fields(
                                    new String[] {"ti", "si", "i", "bi", "f", "d", "de"},
                                    new DataType[] {
                                        DataTypes.TINYINT(),
                                        DataTypes.SMALLINT(),
                                        DataTypes.INT(),
                                        DataTypes.BIGINT(),
                                        DataTypes.FLOAT(),
                                        DataTypes.DOUBLE(),
                                        DataTypes.DECIMAL(10, 5)
                                    })
                            .field("cost", DataTypes.DOUBLE(), "`d` * `bi`")
                            .build();
            assertEquals(expectedSchema, catalogBaseTable.getSchema());

            // table with character types
            tablePath = new ObjectPath(db1, "generic2");
            hiveTable =
                    org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                            tablePath.getDatabaseName(), tablePath.getObjectName());
            hiveTable.setDbName(tablePath.getDatabaseName());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().putAll(getBatchTableProperties());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().put("flink.generic.table.schema.0.name", "c");
            hiveTable.getParameters().put("flink.generic.table.schema.0.data-type", "CHAR(265)");
            hiveTable.getParameters().put("flink.generic.table.schema.1.name", "vc");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.1.data-type", "VARCHAR(65536)");
            hiveTable.getParameters().put("flink.generic.table.schema.2.name", "s");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.2.data-type", "VARCHAR(2147483647)");
            hiveTable.getParameters().put("flink.generic.table.schema.3.name", "b");
            hiveTable.getParameters().put("flink.generic.table.schema.3.data-type", "BINARY(1)");
            hiveTable.getParameters().put("flink.generic.table.schema.4.name", "vb");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.4.data-type", "VARBINARY(255)");
            hiveTable.getParameters().put("flink.generic.table.schema.5.name", "bs");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.5.data-type", "VARBINARY(2147483647)");
            hiveTable.getParameters().put("flink.generic.table.schema.6.name", "len");
            hiveTable.getParameters().put("flink.generic.table.schema.6.expr", "CHAR_LENGTH(`s`)");
            hiveTable.getParameters().put("flink.generic.table.schema.6.data-type", "INT");
            ((HiveCatalog) catalog).client.createTable(hiveTable);
            catalogBaseTable = catalog.getTable(tablePath);
            expectedSchema =
                    TableSchema.builder()
                            .fields(
                                    new String[] {"c", "vc", "s", "b", "vb", "bs"},
                                    new DataType[] {
                                        DataTypes.CHAR(265),
                                        DataTypes.VARCHAR(65536),
                                        DataTypes.STRING(),
                                        DataTypes.BINARY(1),
                                        DataTypes.VARBINARY(255),
                                        DataTypes.BYTES()
                                    })
                            .field("len", DataTypes.INT(), "CHAR_LENGTH(`s`)")
                            .build();
            assertEquals(expectedSchema, catalogBaseTable.getSchema());

            // table with date/time types
            tablePath = new ObjectPath(db1, "generic3");
            hiveTable =
                    org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                            tablePath.getDatabaseName(), tablePath.getObjectName());
            hiveTable.setDbName(tablePath.getDatabaseName());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().putAll(getBatchTableProperties());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().put("flink.generic.table.schema.0.name", "dt");
            hiveTable.getParameters().put("flink.generic.table.schema.0.data-type", "DATE");
            hiveTable.getParameters().put("flink.generic.table.schema.1.name", "t");
            hiveTable.getParameters().put("flink.generic.table.schema.1.data-type", "TIME(0)");
            hiveTable.getParameters().put("flink.generic.table.schema.2.name", "ts");
            hiveTable.getParameters().put("flink.generic.table.schema.2.data-type", "TIMESTAMP(3)");
            hiveTable.getParameters().put("flink.generic.table.schema.3.name", "tstz");
            hiveTable
                    .getParameters()
                    .put(
                            "flink.generic.table.schema.3.data-type",
                            "TIMESTAMP(6) WITH LOCAL TIME ZONE");
            hiveTable.getParameters().put("flink.generic.table.schema.watermark.0.rowtime", "ts");
            hiveTable
                    .getParameters()
                    .put(
                            "flink.generic.table.schema.watermark.0.strategy.data-type",
                            "TIMESTAMP(3)");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.watermark.0.strategy.expr", "ts");
            ((HiveCatalog) catalog).client.createTable(hiveTable);
            catalogBaseTable = catalog.getTable(tablePath);
            expectedSchema =
                    TableSchema.builder()
                            .fields(
                                    new String[] {"dt", "t", "ts", "tstz"},
                                    new DataType[] {
                                        DataTypes.DATE(),
                                        DataTypes.TIME(),
                                        DataTypes.TIMESTAMP(3),
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
                                    })
                            .watermark("ts", "ts", DataTypes.TIMESTAMP(3))
                            .build();
            assertEquals(expectedSchema, catalogBaseTable.getSchema());

            // table with complex/misc types
            tablePath = new ObjectPath(db1, "generic4");
            hiveTable =
                    org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                            tablePath.getDatabaseName(), tablePath.getObjectName());
            hiveTable.setDbName(tablePath.getDatabaseName());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().putAll(getBatchTableProperties());
            hiveTable.setTableName(tablePath.getObjectName());
            hiveTable.getParameters().put("flink.generic.table.schema.0.name", "a");
            hiveTable.getParameters().put("flink.generic.table.schema.0.data-type", "ARRAY<INT>");
            hiveTable.getParameters().put("flink.generic.table.schema.1.name", "m");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.1.data-type", "MAP<BIGINT, TIMESTAMP(6)>");
            hiveTable.getParameters().put("flink.generic.table.schema.2.name", "mul");
            hiveTable
                    .getParameters()
                    .put("flink.generic.table.schema.2.data-type", "MULTISET<DOUBLE>");
            hiveTable.getParameters().put("flink.generic.table.schema.3.name", "r");
            hiveTable
                    .getParameters()
                    .put(
                            "flink.generic.table.schema.3.data-type",
                            "ROW<`f1` INT, `f2` VARCHAR(2147483647)>");
            hiveTable.getParameters().put("flink.generic.table.schema.4.name", "b");
            hiveTable.getParameters().put("flink.generic.table.schema.4.data-type", "BOOLEAN");
            hiveTable.getParameters().put("flink.generic.table.schema.5.name", "ts");
            hiveTable.getParameters().put("flink.generic.table.schema.5.data-type", "TIMESTAMP(3)");
            hiveTable.getParameters().put("flink.generic.table.schema.watermark.0.rowtime", "ts");
            hiveTable
                    .getParameters()
                    .put(
                            "flink.generic.table.schema.watermark.0.strategy.data-type",
                            "TIMESTAMP(3)");
            hiveTable
                    .getParameters()
                    .put(
                            "flink.generic.table.schema.watermark.0.strategy.expr",
                            "`ts` - INTERVAL '5' SECOND");
            ((HiveCatalog) catalog).client.createTable(hiveTable);
            catalogBaseTable = catalog.getTable(tablePath);
            expectedSchema =
                    TableSchema.builder()
                            .fields(
                                    new String[] {"a", "m", "mul", "r", "b", "ts"},
                                    new DataType[] {
                                        DataTypes.ARRAY(DataTypes.INT()),
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.TIMESTAMP()),
                                        DataTypes.MULTISET(DataTypes.DOUBLE()),
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f1", DataTypes.INT()),
                                                DataTypes.FIELD("f2", DataTypes.STRING())),
                                        DataTypes.BOOLEAN(),
                                        DataTypes.TIMESTAMP(3)
                                    })
                            .watermark("ts", "`ts` - INTERVAL '5' SECOND", DataTypes.TIMESTAMP(3))
                            .build();
            assertEquals(expectedSchema, catalogBaseTable.getSchema());
        } finally {
            catalog.dropDatabase(db1, true, true);
        }
    }

    @Test
    public void testFunctionCompatibility() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        // create a function with old prefix 'flink:' and make sure we can properly retrieve it
        ((HiveCatalog) catalog)
                .client.createFunction(
                        new Function(
                                path1.getObjectName().toLowerCase(),
                                path1.getDatabaseName(),
                                "flink:class.name",
                                null,
                                PrincipalType.GROUP,
                                (int) (System.currentTimeMillis() / 1000),
                                FunctionType.JAVA,
                                new ArrayList<>()));
        CatalogFunction catalogFunction = catalog.getFunction(path1);
        assertEquals("class.name", catalogFunction.getClassName());
        assertEquals(FunctionLanguage.JAVA, catalogFunction.getFunctionLanguage());
    }

    // ------ functions ------

    @Test
    public void testFunctionWithNonExistClass() throws Exception {
        // to make sure hive catalog doesn't check function class
        catalog.createDatabase(db1, createDb(), false);
        CatalogFunction catalogFunction =
                new CatalogFunctionImpl("non.exist.scala.class", FunctionLanguage.SCALA);
        catalog.createFunction(path1, catalogFunction, false);
        assertEquals(catalogFunction.getClassName(), catalog.getFunction(path1).getClassName());
        assertEquals(
                catalogFunction.getFunctionLanguage(),
                catalog.getFunction(path1).getFunctionLanguage());
        // alter the function
        catalogFunction = new CatalogFunctionImpl("non.exist.java.class", FunctionLanguage.JAVA);
        catalog.alterFunction(path1, catalogFunction, false);
        assertEquals(catalogFunction.getClassName(), catalog.getFunction(path1).getClassName());
        assertEquals(
                catalogFunction.getFunctionLanguage(),
                catalog.getFunction(path1).getFunctionLanguage());

        catalogFunction =
                new CatalogFunctionImpl("non.exist.python.class", FunctionLanguage.PYTHON);
        catalog.alterFunction(path1, catalogFunction, false);
        assertEquals(catalogFunction.getClassName(), catalog.getFunction(path1).getClassName());
        assertEquals(
                catalogFunction.getFunctionLanguage(),
                catalog.getFunction(path1).getFunctionLanguage());
    }

    // ------ partitions ------

    @Test
    public void testCreatePartition() throws Exception {}

    @Test
    public void testCreatePartition_TableNotExistException() throws Exception {}

    @Test
    public void testCreatePartition_TableNotPartitionedException() throws Exception {}

    @Test
    public void testCreatePartition_PartitionSpecInvalidException() throws Exception {}

    @Test
    public void testCreatePartition_PartitionAlreadyExistsException() throws Exception {}

    @Test
    public void testCreatePartition_PartitionAlreadyExists_ignored() throws Exception {}

    @Test
    public void testDropPartition() throws Exception {}

    @Test
    public void testDropPartition_TableNotExist() throws Exception {}

    @Test
    public void testDropPartition_TableNotPartitioned() throws Exception {}

    @Test
    public void testDropPartition_PartitionSpecInvalid() throws Exception {}

    @Test
    public void testDropPartition_PartitionNotExist() throws Exception {}

    @Test
    public void testDropPartition_PartitionNotExist_ignored() throws Exception {}

    @Test
    public void testAlterPartition() throws Exception {}

    @Test
    public void testAlterPartition_TableNotExist() throws Exception {}

    @Test
    public void testAlterPartition_TableNotPartitioned() throws Exception {}

    @Test
    public void testAlterPartition_PartitionSpecInvalid() throws Exception {}

    @Test
    public void testAlterPartition_PartitionNotExist() throws Exception {}

    @Test
    public void testAlterPartition_PartitionNotExist_ignored() throws Exception {}

    @Test
    public void testGetPartition_TableNotExist() throws Exception {}

    @Test
    public void testGetPartition_TableNotPartitioned() throws Exception {}

    @Test
    public void testGetPartition_PartitionSpecInvalid_invalidPartitionSpec() throws Exception {}

    @Test
    public void testGetPartition_PartitionSpecInvalid_sizeNotEqual() throws Exception {}

    @Test
    public void testGetPartition_PartitionNotExist() throws Exception {}

    @Test
    public void testPartitionExists() throws Exception {}

    @Test
    public void testListPartitionPartialSpec() throws Exception {}

    @Override
    public void testGetPartitionStats() throws Exception {}

    @Override
    public void testAlterPartitionTableStats() throws Exception {}

    @Override
    public void testAlterTableStats_partitionedTable() throws Exception {}

    // ------ test utils ------

    @Override
    protected boolean isGeneric() {
        return true;
    }

    @Override
    public CatalogPartition createPartition() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CatalogFunction createFunction() {
        return new CatalogFunctionImpl(TestGenericUDF.class.getCanonicalName());
    }

    @Override
    protected CatalogFunction createAnotherFunction() {
        return new CatalogFunctionImpl(
                TestSimpleUDF.class.getCanonicalName(), FunctionLanguage.SCALA);
    }
}
