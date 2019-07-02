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
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.VarBinaryType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test for HiveCatalog on generic metadata.
 */
public class HiveCatalogGenericMetadataTest extends CatalogTestBase {

	@BeforeClass
	public static void init() {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	// ------ TODO: Move data types tests to its own test class as it's shared between generic metadata and hive metadata
	// ------ data types ------

	@Test
	public void testDataTypes() throws Exception {
		// TODO: the following Hive types are not supported in Flink yet, including MAP, STRUCT
		DataType[] types = new DataType[] {
			DataTypes.TINYINT(),
			DataTypes.SMALLINT(),
			DataTypes.INT(),
			DataTypes.BIGINT(),
			DataTypes.FLOAT(),
			DataTypes.DOUBLE(),
			DataTypes.BOOLEAN(),
			DataTypes.STRING(),
			DataTypes.BYTES(),
			DataTypes.DATE(),
			DataTypes.TIMESTAMP(),
			DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH),
			DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH),
			DataTypes.DECIMAL(5, 3)
		};

		verifyDataTypes(types);
	}

	@Test
	public void testNonExactlyMatchedDataTypes() throws Exception {
		DataType[] types = new DataType[] {
			DataTypes.BINARY(BinaryType.MAX_LENGTH),
			DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH)
		};

		CatalogTable table = createCatalogTable(types);

		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		Arrays.equals(
			new DataType[] {DataTypes.BYTES(), DataTypes.BYTES()},
			catalog.getTable(path1).getSchema().getFieldDataTypes());
	}

	@Test
	public void testCharTypeLength() throws Exception {
		DataType[] types = new DataType[] {
			DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH + 1)
		};

		exception.expect(CatalogException.class);
		exception.expectMessage("HiveCatalog doesn't support char type with length of '256'. The maximum length is 255");
		verifyDataTypes(types);
	}

	@Test
	public void testVarCharTypeLength() throws Exception {
		DataType[] types = new DataType[] {
			DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH + 1)
		};

		exception.expect(CatalogException.class);
		exception.expectMessage("HiveCatalog doesn't support varchar type with length of '65536'. The maximum length is 65535");
		verifyDataTypes(types);
	}

	@Test
	public void testComplexDataTypes() throws Exception {
		DataType[] types = new DataType[]{
			DataTypes.ARRAY(DataTypes.DOUBLE()),
			DataTypes.MAP(DataTypes.FLOAT(), DataTypes.BIGINT()),
			DataTypes.ROW(
				DataTypes.FIELD("0", DataTypes.BOOLEAN()),
				DataTypes.FIELD("1", DataTypes.BOOLEAN()),
				DataTypes.FIELD("2", DataTypes.DATE())),

			// nested complex types
			DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
			DataTypes.MAP(DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
			DataTypes.ROW(
				DataTypes.FIELD("3", DataTypes.ARRAY(DataTypes.DECIMAL(5, 3))),
				DataTypes.FIELD("4", DataTypes.MAP(DataTypes.TINYINT(), DataTypes.SMALLINT())),
				DataTypes.FIELD("5", DataTypes.ROW(DataTypes.FIELD("3", DataTypes.TIMESTAMP())))
			)
		};

		verifyDataTypes(types);
	}

	private CatalogTable createCatalogTable(DataType[] types) {
		String[] colNames = new String[types.length];

		for (int i = 0; i < types.length; i++) {
			colNames[i] = String.format("%s_%d", types[i].toString().toLowerCase(), i);
		}

		TableSchema schema = TableSchema.builder()
			.fields(colNames, types)
			.build();

		return new CatalogTableImpl(
			schema,
			getBatchTableProperties(),
			TEST_COMMENT
		);
	}

	private void verifyDataTypes(DataType[] types) throws Exception {
		CatalogTable table = createCatalogTable(types);

		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertEquals(table.getSchema(), catalog.getTable(path1).getSchema());
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
