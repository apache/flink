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
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveCatalog on generic metadata.
 */
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

		TableSchema tableSchema = TableSchema.builder()
				.fields(new String[]{"col1", "col2", "col3"},
						new DataType[]{DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(9)})
				.watermark("col3", "col3", DataTypes.TIMESTAMP(9))
				.build();

		ObjectPath tablePath = new ObjectPath(db1, "generic_table");
		try {
			catalog.createTable(tablePath,
					new CatalogTableImpl(tableSchema, getBatchTableProperties(), TEST_COMMENT),
					false);

			assertEquals(tableSchema, catalog.getTable(tablePath).getSchema());
		} finally {
			catalog.dropTable(tablePath, true);
		}
	}

	@Test
	public void testTableSchemaCompatibility() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		ObjectPath tablePath = new ObjectPath(db1, "generic110");

		// create a table with old schema properties
		Table hiveTable = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(tablePath.getDatabaseName(),
				tablePath.getObjectName());
		// create table generic110 (c char(265), vc varchar(65536), ts timestamp(3), watermark for ts as ts)
		hiveTable.setDbName(tablePath.getDatabaseName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.getParameters().put(CatalogConfig.IS_GENERIC, "true");
		hiveTable.getParameters().put("flink.generic.table.schema.0.name", "c");
		hiveTable.getParameters().put("flink.generic.table.schema.0.data-type", "CHAR(265)");
		hiveTable.getParameters().put("flink.generic.table.schema.1.name", "vc");
		hiveTable.getParameters().put("flink.generic.table.schema.1.data-type", "VARCHAR(65536)");
		hiveTable.getParameters().put("flink.generic.table.schema.2.name", "ts");
		hiveTable.getParameters().put("flink.generic.table.schema.2.data-type", "TIMESTAMP(3)");
		hiveTable.getParameters().put("flink.generic.table.schema.watermark.0.rowtime", "ts");
		hiveTable.getParameters().put("flink.generic.table.schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");
		hiveTable.getParameters().put("flink.generic.table.schema.watermark.0.strategy.expr", "ts");

		try {
			((HiveCatalog) catalog).client.createTable(hiveTable);
			CatalogBaseTable catalogBaseTable = catalog.getTable(tablePath);
			assertTrue(Boolean.parseBoolean(catalogBaseTable.getOptions().get(CatalogConfig.IS_GENERIC)));
			TableSchema expectedSchema = TableSchema.builder()
					.fields(new String[]{"c", "vc", "ts"},
							new DataType[]{DataTypes.CHAR(265), DataTypes.VARCHAR(65536), DataTypes.TIMESTAMP(3)})
					.watermark("ts", "ts", DataTypes.TIMESTAMP(3))
					.build();
			assertEquals(expectedSchema, catalogBaseTable.getSchema());
		} finally {
			catalog.dropTable(tablePath, true);
		}
	}

	// ------ partitions ------

	@Test
	public void testCreatePartition() throws Exception {
	}

	@Test
	public void testCreatePartition_TableNotExistException() throws Exception {
	}

	@Test
	public void testCreatePartition_TableNotPartitionedException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionSpecInvalidException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExistsException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExists_ignored() throws Exception {
	}

	@Test
	public void testDropPartition() throws Exception {
	}

	@Test
	public void testDropPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testDropPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionSpecInvalid() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionNotExist_ignored() throws Exception {
	}

	@Test
	public void testAlterPartition() throws Exception {
	}

	@Test
	public void testAlterPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testAlterPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionSpecInvalid() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionNotExist_ignored() throws Exception {
	}

	@Test
	public void testGetPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testGetPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionSpecInvalid_invalidPartitionSpec() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionSpecInvalid_sizeNotEqual() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testPartitionExists() throws Exception {
	}

	@Test
	public void testListPartitionPartialSpec() throws Exception {
	}

	@Override
	public void testGetPartitionStats() throws Exception {
	}

	@Override
	public void testAlterPartitionTableStats() throws Exception {
	}

	@Override
	public void testAlterTableStats_partitionedTable() throws Exception {
	}

	// ------ test utils ------

	@Override
	protected boolean isGeneric() {
		return true;
	}

	@Override
	public CatalogPartition createPartition() {
		throw new UnsupportedOperationException();
	}
}
