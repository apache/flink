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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base for unit tests of a specific catalog, like FlinkInMemoryCatalog and HiveCatalog.
 */
public abstract class CatalogTestBase {
	protected final String db1 = "db1";
	protected final String db2 = "db2";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db2, t2);
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistTablePath = ObjectPath.fromString("db1.nonexist");

	protected static ReadableWritableCatalog catalog;

	public abstract String getTableType();

	// ------ tables ------

	@Test
	public void testCreateTable() {
		assertTrue(catalog.listAllTables().isEmpty());
		assertFalse(catalog.tableExists(path1));

		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		List<ObjectPath> tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		List<ObjectPath> s1Tables = catalog.listTables(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		s1Tables = catalog.listTables(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testCreateTable_DatabaseNotExistException() {
		assertFalse(catalog.dbExists(db1));

		catalog.createTable(nonExistTablePath, createTable(), false);
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		assertEquals(table, catalog.getTable(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testGetTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.getTable(nonExistTablePath);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetTable_TableNotExistException_NoDb() {
		catalog.getTable(nonExistTablePath);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));

		// Partitioned table
		catalog.createTable(path1, createPartitionedTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropTable_TableNotExistException() {
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistTablePath, true);
	}

	@Test
	public void testAlterTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		newTable = createAnotherPartitionedTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));
	}

	@Test
	public void testAlterTable_withTableStats() {
		CatalogTable table = createTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterTable_TableNotExistException() {
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistTablePath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistTablePath));
	}

	@Test
	public void testTableExists() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	// ------ table and column stats ------

	@Test (expected = TableNotExistException.class)
	public void testGetTableStats_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		catalog.getTableStats(path1);
	}

	@Test
	public void testAlterTableStats() {
		// Non-partitioned table
		catalog.createDatabase(db1, createDb(), false);

		TableSchema schema = new TableSchema(
			new String[] {
				"1",
				"2",
				"3",
				"4",
				"5",
				"6",
				"7",
				"8",
				"9",
				"10",
				"11",
				"12"
			},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.BOOLEAN,
				DataTypes.BYTE,
				DataTypes.SHORT,
				DataTypes.INT,
				DataTypes.LONG,
				DataTypes.FLOAT,
				DataTypes.DOUBLE,
				DataTypes.DATE,
				DataTypes.CHAR,
				new DecimalType(6, 2),
				TimestampType.TIMESTAMP,
			}
		);

		CatalogTable table = CatalogTestUtil.createCatalogTable(
			getTableType(),
			schema,
			getTableProperties());
		catalog.createTable(path1, table, false);

		TableStats tableStats = new TableStats(100L, new HashMap<String, ColumnStats>() {{
			// StringType
			put("1", new ColumnStats(11L, 1L, 1.1, 1, null, null));
			// BooleanType
			put("2", new ColumnStats(null, 2L, null, null, null, null));
			// ByteType
			put("3", new ColumnStats(13L, 3L, null, null, Byte.valueOf((byte) 3), Byte.valueOf((byte) 2)));
			// ShortType
			put("4", new ColumnStats(15L, 5L, null, null, Short.valueOf((short) 4), Short.valueOf((short) 3)));
			// IntType
			put("5", new ColumnStats(14L, 4L, null, null, Integer.valueOf(5), Integer.valueOf(4)));
			// LongType
			put("6", new ColumnStats(16L, 7L, null, null, Long.valueOf(6L), Long.valueOf(5L)));
			// FloatType
			put("7", new ColumnStats(17L, 8L, null, null, Float.valueOf(8.8f), Float.valueOf(7.7f)));
			// DoubleType
			put("8", new ColumnStats(18L, 9L, null, null, Double.valueOf(9.9d), Double.valueOf(8.8d)));
			// DateType
			put("9", new ColumnStats(19L, 10L, null, null, new Date(1547529235000L), new Date(1540529200000L)));
			// CharType
			put("10", new ColumnStats(19L, 10L, 1.0, 1, null, null));
			// DecimalType
			put("11", new ColumnStats(19L, 10L, null, null, Decimal.fromLong(999999, 6, 3), Decimal.fromLong(666666, 6, 3)));
			// TimetampType.Timestamp
			put("12", new ColumnStats(19L, 10L, null, null, new Timestamp(1547529235000L), new Timestamp(1540529200000L)));
		}});

		catalog.alterTableStats(path1, tableStats, false);
		TableStats actual = catalog.getTableStats(path1);

		assertEquals(tableStats.toString(), actual.toString());
	}

	@Test
	public void testAlterTableStats_partitionedTable() {
		// alterTableStats() should do nothing for partitioned tables
		// getTableStats() should return empty column stats for partitioned tables
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		TableStats stats = new TableStats(0L, new HashMap<String, ColumnStats>() {{
			put("first", new ColumnStats(11L, 1L, 1.1, 1, null, null));
			put("second", new ColumnStats(14L, 4L, null, null, 5, 4));
			put("third", new ColumnStats(11L, 1L, 1.1, 1, null, null));
		}});

		catalog.alterTableStats(path1, stats, false);

		assertEquals(new TableStats().toString(), catalog.getTableStats(path1).toString());
	}

	@Test (expected = TableNotExistException.class)
	public void testAlterTableStats_TableNotExistException() {
		catalog.alterTableStats(new ObjectPath(catalog.getDefaultDatabaseName(), "nonexist"), null, false);
	}

	@Test (expected = TableNotExistException.class)
	public void testAlterTableStats_TableNotExistExceptio_2() {
		catalog.alterTableStats(new ObjectPath("non", "exist"), null, false);
	}

	@Test
	public void testAlterTableStats_TableNotExistExceptio_ignore() {
		catalog.alterTableStats(new ObjectPath("non", "exist"), null, true);
	}

	// ------ databases ------

	@Test
	public void testCreateDb() {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(2, catalog.listDatabases().size());
	}

	@Test(expected = DatabaseAlreadyExistException.class)
	public void testCreateDb_DatabaseAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createDatabase(db1, createDb(), false);
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExist_ignored() {
		CatalogDatabase cd1 = createDb();
		catalog.createDatabase(db1, cd1, false);
		List<String> dbs = catalog.listDatabases();

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));

		catalog.createDatabase(db1, createAnotherDb(), true);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testGetDb_DatabaseNotExistException() {
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testDropDb() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test (expected = DatabaseNotExistException.class)
	public void testDropDb_DatabaseNotExistException() {
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropDb_DatabaseNotExist_Ignore() {
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testAlterDb() {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		assertFalse(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));
		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(newDb.getProperties().entrySet()));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testAlterDb_DatabaseNotExistException() {
		catalog.alterDatabase("nonexistent", createDb(), false);
	}

	@Test
	public void testAlterDb_DatabaseNotExist_ignored() {
		catalog.alterDatabase("nonexistent", createDb(), true);

		assertFalse(catalog.dbExists("nonexistent"));
	}

	@Test
	public void testDbExists() {
		assertFalse(catalog.dbExists("nonexistent"));

		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.dbExists(db1));
	}

	// ------ partitions ------

	@Test
	public void testCreatePartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		assertTrue(catalog.listPartitions(path1).isEmpty());

		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1, createPartitionSpecSubset()));
		assertEquals(createPartition(), catalog.getPartition(path1, createPartitionSpec()));

		catalog.createPartition(path1, createAnotherPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()), catalog.listPartitions(path1, createPartitionSpecSubset()));
		assertEquals(createAnotherPartition(), catalog.getPartition(path1, createAnotherPartitionSpec()));
	}

	@Test (expected = TableNotExistException.class)
	public void testCreateParition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test (expected = TableNotPartitionedException.class)
	public void testCreateParition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test (expected = PartitionAlreadyExistException.class)
	public void testCreateParition_PartitionAlreadExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test
	public void testCreateParition_PartitionAlreadExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);
		catalog.createPartition(path1, createPartition(), true);
	}

	@Test
	public void testDropPartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));

		catalog.dropPartition(path1, createPartitionSpec(), false);

		assertEquals(Arrays.asList(), catalog.listPartitions(path1));
	}

	@Test (expected = TableNotExistException.class)
	public void testDropPartition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test (expected = TableNotPartitionedException.class)
	public void testDropPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test (expected = PartitionNotExistException.class)
	public void testDropPartition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test
	public void testDropPartition_PartitionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.dropPartition(path1, createPartitionSpec(), true);
	}

	@Test
	public void testAlterPartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
		assertEquals(createPartition(), cp);
		assertNull(cp.getProperties().get("k"));

		Map<String, String> partitionProperties = getTableProperties();
		partitionProperties.put("k", "v");

		CatalogPartition another = createPartition(cp.getPartitionSpec(), partitionProperties);
		catalog.alterPartition(path1, another, false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		cp = catalog.getPartition(path1, createPartitionSpec());
		assertEquals(another, cp);
		assertEquals("v", cp.getProperties().get("k"));
	}

	@Test (expected = TableNotExistException.class)
	public void testAlterPartition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test (expected = TableNotPartitionedException.class)
	public void testAlterPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test (expected = PartitionNotExistException.class)
	public void testAlterPartition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.alterPartition(path1, createPartition(), true);
	}

	@Test (expected = TableNotExistException.class)
	public void testGetPartition_TableNotExistException() {
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test (expected = TableNotPartitionedException.class)
	public void testGetPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test (expected = PartitionNotExistException.class)
	public void testGetParition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test
	public void testPartitionExists() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertTrue(catalog.partitionExists(path1, createPartitionSpec()));
		assertFalse(catalog.partitionExists(path2, createPartitionSpec()));
		assertFalse(catalog.partitionExists(ObjectPath.fromString("non.exist"), createPartitionSpec()));
	}

	// ------ utilities ------

	protected CatalogTable createTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			getTableProperties());
	}

	protected CatalogTable createAnotherTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			getTableProperties());
	}

	protected CatalogTable createPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			new TableStats(),
			getTableProperties(),
			createPartitionCols());
	}

	protected CatalogTable createAnotherPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			new TableStats(),
			getTableProperties(),
			createPartitionCols());
	}

	private LinkedHashSet<String> createPartitionCols() {
		return new LinkedHashSet<String>() {{
			add("second");
			add("third");
		}};
	}

	protected CatalogPartition.PartitionSpec createPartitionSpec() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2000");
				put("second", "bob");
			}});
	}

	protected CatalogPartition.PartitionSpec createAnotherPartitionSpec() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2010");
				put("second", "bob");
			}});
	}

	protected CatalogPartition.PartitionSpec createPartitionSpecSubset() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("second", "bob");
			}});
	}

	protected CatalogPartition createPartition() {
		return createPartition(createPartitionSpec(), getTableProperties());
	}

	protected CatalogPartition createAnotherPartition() {
		return createPartition(createAnotherPartitionSpec(), getTableProperties());
	}

	protected CatalogPartition createPartition(CatalogPartition.PartitionSpec partitionSpec, Map<String, String> partitionProperties) {
		return new CatalogPartition(partitionSpec, partitionProperties);
	}

	protected CatalogDatabase createDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
		}});
	}

	protected CatalogDatabase createAnotherDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
		}});
	}

	private TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.INT,
				DataTypes.STRING,
			}
		);
	}

	private TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.STRING,  // different from create table instance.
				DataTypes.STRING
			}
		);
	}

	protected Map<String, String> getTableProperties() {
		return new HashMap<>();
	}
}
