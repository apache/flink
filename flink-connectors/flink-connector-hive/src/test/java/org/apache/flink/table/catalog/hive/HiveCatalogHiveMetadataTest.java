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

import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for HiveCatalog on Hive metadata. */
public class HiveCatalogHiveMetadataTest extends HiveCatalogMetadataTestBase {

    @BeforeClass
    public static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    // =====================
    // HiveCatalog doesn't support streaming table operation. Ignore this test in CatalogTest.
    // =====================

    public void testCreateTable_Streaming() throws Exception {}

    @Test
    // verifies that input/output formats and SerDe are set for Hive tables
    public void testCreateTable_StorageFormatSet() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        Table hiveTable = ((HiveCatalog) catalog).getHiveTable(path1);
        String inputFormat = hiveTable.getSd().getInputFormat();
        String outputFormat = hiveTable.getSd().getOutputFormat();
        String serde = hiveTable.getSd().getSerdeInfo().getSerializationLib();
        assertFalse(StringUtils.isNullOrWhitespaceOnly(inputFormat));
        assertFalse(StringUtils.isNullOrWhitespaceOnly(outputFormat));
        assertFalse(StringUtils.isNullOrWhitespaceOnly(serde));
    }

    // ------ table and column stats ------

    @Test
    public void testViewCompatibility() throws Exception {
        // we always store view schema via properties now
        // make sure non-generic views created previously can still be used
        catalog.createDatabase(db1, createDb(), false);
        Table hiveView =
                org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                        path1.getDatabaseName(), path1.getObjectName());
        // mark as a view
        hiveView.setTableType(TableType.VIRTUAL_VIEW.name());
        final String originQuery = "view origin query";
        final String expandedQuery = "view expanded query";
        hiveView.setViewOriginalText(originQuery);
        hiveView.setViewExpandedText(expandedQuery);
        // set schema in SD
        Schema schema =
                Schema.newBuilder()
                        .fromFields(
                                new String[] {"i", "s"},
                                new AbstractDataType[] {DataTypes.INT(), DataTypes.STRING()})
                        .build();
        List<FieldSchema> fields = new ArrayList<>();
        for (Schema.UnresolvedColumn column : schema.getColumns()) {
            String name = column.getName();
            DataType type = (DataType) ((Schema.UnresolvedPhysicalColumn) column).getDataType();
            fields.add(
                    new FieldSchema(
                            name, HiveTypeUtil.toHiveTypeInfo(type, true).getTypeName(), null));
        }
        hiveView.getSd().setCols(fields);
        // test mark as non-generic with is_generic
        hiveView.getParameters().put(CatalogPropertiesUtil.IS_GENERIC, "false");
        // add some other properties
        hiveView.getParameters().put("k1", "v1");

        ((HiveCatalog) catalog).client.createTable(hiveView);
        CatalogBaseTable baseTable = catalog.getTable(path1);
        assertTrue(baseTable instanceof CatalogView);
        CatalogView catalogView = (CatalogView) baseTable;
        assertEquals(schema, catalogView.getUnresolvedSchema());
        assertEquals(originQuery, catalogView.getOriginalQuery());
        assertEquals(expandedQuery, catalogView.getExpandedQuery());
        assertEquals("v1", catalogView.getOptions().get("k1"));

        // test mark as non-generic with connector
        hiveView.setDbName(path3.getDatabaseName());
        hiveView.setTableName(path3.getObjectName());
        hiveView.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC);
        hiveView.getParameters().put(CONNECTOR.key(), IDENTIFIER);

        ((HiveCatalog) catalog).client.createTable(hiveView);
        baseTable = catalog.getTable(path3);
        assertTrue(baseTable instanceof CatalogView);
        catalogView = (CatalogView) baseTable;
        assertEquals(schema, catalogView.getUnresolvedSchema());
        assertEquals(originQuery, catalogView.getOriginalQuery());
        assertEquals(expandedQuery, catalogView.getExpandedQuery());
        assertEquals("v1", catalogView.getOptions().get("k1"));
    }

    @Test
    public void testAlterTableColumnStatistics() throws Exception {
        String hiveVersion = ((HiveCatalog) catalog).getHiveVersion();
        boolean supportDateStats = hiveVersion.compareTo(HiveShimLoader.HIVE_VERSION_V1_2_0) >= 0;
        catalog.createDatabase(db1, createDb(), false);
        TableSchema.Builder builder =
                TableSchema.builder()
                        .field("first", DataTypes.STRING())
                        .field("second", DataTypes.INT())
                        .field("third", DataTypes.BOOLEAN())
                        .field("fourth", DataTypes.DOUBLE())
                        .field("fifth", DataTypes.BIGINT())
                        .field("sixth", DataTypes.BYTES())
                        .field("seventh", DataTypes.DECIMAL(10, 3))
                        .field("eighth", DataTypes.DECIMAL(30, 3));
        if (supportDateStats) {
            builder.field("ninth", DataTypes.DATE());
        }
        TableSchema tableSchema = builder.build();
        CatalogTable catalogTable =
                new CatalogTableImpl(tableSchema, getBatchTableProperties(), TEST_COMMENT);
        catalog.createTable(path1, catalogTable, false);
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
        columnStatisticsDataBaseMap.put(
                "first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
        columnStatisticsDataBaseMap.put(
                "second", new CatalogColumnStatisticsDataLong(0L, 1000L, 3L, 0L));
        columnStatisticsDataBaseMap.put(
                "third", new CatalogColumnStatisticsDataBoolean(15L, 20L, 3L));
        columnStatisticsDataBaseMap.put(
                "fourth", new CatalogColumnStatisticsDataDouble(15.02, 20.01, 3L, 10L));
        columnStatisticsDataBaseMap.put(
                "fifth", new CatalogColumnStatisticsDataLong(0L, 20L, 3L, 2L));
        columnStatisticsDataBaseMap.put(
                "sixth", new CatalogColumnStatisticsDataBinary(150L, 20D, 3L));
        columnStatisticsDataBaseMap.put(
                "seventh", new CatalogColumnStatisticsDataDouble(1.23, 99.456, 100L, 0L));
        columnStatisticsDataBaseMap.put(
                "eighth", new CatalogColumnStatisticsDataDouble(0.123, 123456.789, 5723L, 19L));
        if (supportDateStats) {
            columnStatisticsDataBaseMap.put(
                    "ninth",
                    new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 132L, 0L));
        }
        CatalogColumnStatistics catalogColumnStatistics =
                new CatalogColumnStatistics(columnStatisticsDataBaseMap);
        catalog.alterTableColumnStatistics(path1, catalogColumnStatistics, false);

        checkEquals(catalogColumnStatistics, catalog.getTableColumnStatistics(path1));
    }

    @Test
    public void testAlterPartitionColumnStatistics() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable catalogTable = createPartitionedTable();
        catalog.createTable(path1, catalogTable, false);
        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(
                        new HashMap<String, String>() {
                            {
                                put("second", "2010-04-21 09:45:00");
                                put("third", "2000");
                            }
                        });
        catalog.createPartition(path1, partitionSpec, createPartition(), true);
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
        columnStatisticsDataBaseMap.put(
                "first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
        CatalogColumnStatistics catalogColumnStatistics =
                new CatalogColumnStatistics(columnStatisticsDataBaseMap);
        catalog.alterPartitionColumnStatistics(
                path1, partitionSpec, catalogColumnStatistics, false);

        checkEquals(
                catalogColumnStatistics,
                catalog.getPartitionColumnStatistics(path1, partitionSpec));
    }

    @Test
    public void testHiveStatistics() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        checkStatistics(0, -1);
        checkStatistics(1, 1);
        checkStatistics(1000, 1000);
    }

    @Test
    public void testCreateTableWithConstraints() throws Exception {
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
        HiveCatalog hiveCatalog = (HiveCatalog) catalog;
        hiveCatalog.createDatabase(db1, createDb(), false);
        TableSchema.Builder builder = TableSchema.builder();
        builder.fields(
                new String[] {"x", "y", "z"},
                new DataType[] {
                    DataTypes.INT().notNull(), DataTypes.TIMESTAMP(9).notNull(), DataTypes.BIGINT()
                });
        builder.primaryKey("pk_name", new String[] {"x"});
        hiveCatalog.createTable(
                path1,
                new CatalogTableImpl(builder.build(), getBatchTableProperties(), null),
                false);
        CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(path1);
        assertTrue("PK not present", catalogTable.getSchema().getPrimaryKey().isPresent());
        UniqueConstraint pk = catalogTable.getSchema().getPrimaryKey().get();
        assertEquals("pk_name", pk.getName());
        assertEquals(Collections.singletonList("x"), pk.getColumns());
        assertFalse(catalogTable.getSchema().getFieldDataTypes()[0].getLogicalType().isNullable());
        assertFalse(catalogTable.getSchema().getFieldDataTypes()[1].getLogicalType().isNullable());
        assertTrue(catalogTable.getSchema().getFieldDataTypes()[2].getLogicalType().isNullable());

        hiveCatalog.dropDatabase(db1, false, true);
    }

    @Override
    @Test
    public void testAlterPartition() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertEquals(
                Collections.singletonList(createPartitionSpec()), catalog.listPartitions(path1));
        CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
        CatalogTestUtil.checkEquals(createPartition(), cp);
        assertNull(cp.getProperties().get("k"));

        CatalogPartition another = createPartition();
        another.getProperties().put("k", "v");
        another.getProperties()
                .put(
                        SqlAlterHiveTable.ALTER_TABLE_OP,
                        SqlAlterHiveTable.AlterTableOp.CHANGE_TBL_PROPS.name());

        catalog.alterPartition(path1, createPartitionSpec(), another, false);

        assertEquals(
                Collections.singletonList(createPartitionSpec()), catalog.listPartitions(path1));

        cp = catalog.getPartition(path1, createPartitionSpec());

        CatalogTestUtil.checkEquals(another, cp);
        assertEquals("v", cp.getProperties().get("k"));
    }

    private void checkStatistics(int inputStat, int expectStat) throws Exception {
        catalog.dropTable(path1, true);

        Map<String, String> properties = new HashMap<>();
        properties.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        properties.put(StatsSetupConst.ROW_COUNT, String.valueOf(inputStat));
        properties.put(StatsSetupConst.NUM_FILES, String.valueOf(inputStat));
        properties.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(inputStat));
        properties.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(inputStat));
        CatalogTable catalogTable =
                new CatalogTableImpl(
                        TableSchema.builder().field("f0", DataTypes.INT()).build(), properties, "");
        catalog.createTable(path1, catalogTable, false);

        CatalogTableStatistics statistics = catalog.getTableStatistics(path1);
        assertEquals(expectStat, statistics.getRowCount());
        assertEquals(expectStat, statistics.getFileCount());
        assertEquals(expectStat, statistics.getRawDataSize());
        assertEquals(expectStat, statistics.getTotalSize());
    }

    // ------ utils ------

    @Override
    protected boolean isGeneric() {
        return false;
    }

    @Override
    public CatalogTable createStreamingTable() {
        throw new UnsupportedOperationException("Hive table cannot be streaming.");
    }

    @Override
    protected CatalogFunction createFunction() {
        return new CatalogFunctionImpl(GenericUDFAbs.class.getName());
    }

    @Override
    protected CatalogFunction createAnotherFunction() {
        return new CatalogFunctionImpl(UDFRand.class.getName());
    }
}
