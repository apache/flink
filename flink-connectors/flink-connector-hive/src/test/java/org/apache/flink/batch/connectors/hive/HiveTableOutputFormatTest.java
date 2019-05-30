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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogTable;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link HiveTableOutputFormatTest}.
 */
public class HiveTableOutputFormatTest {

	private static HiveCatalog hiveCatalog;
	private static HiveConf hiveConf;

	@BeforeClass
	public static void createCatalog() throws IOException {
		hiveConf = HiveTestUtils.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
	}

	@AfterClass
	public static void closeCatalog() {
		if (hiveCatalog != null) {
			hiveCatalog.close();
		}
	}

	@Test
	public void testInsertIntoNonPartitionTable() throws Exception {
		final String dbName = "default";
		final String tblName = "dest";
		ObjectPath tablePath = new ObjectPath(dbName, tblName);
		TableSchema tableSchema = new TableSchema(
				new String[]{"i", "l", "d", "s"},
				new TypeInformation[]{
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO}
		);
		HiveCatalogTable catalogTable = new HiveCatalogTable(tableSchema, new HashMap<>(), "");
		hiveCatalog.createTable(tablePath, catalogTable, false);

		Table hiveTable = hiveCatalog.getHiveTable(tablePath);
		RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
		StorageDescriptor jobSD = hiveTable.getSd().deepCopy();
		jobSD.setLocation(hiveTable.getSd().getLocation() + "/.staging");
		HiveTablePartition hiveTablePartition = new HiveTablePartition(jobSD, null);
		JobConf jobConf = new JobConf(hiveConf);
		HiveTableOutputFormat outputFormat = new HiveTableOutputFormat(jobConf, dbName, tblName,
				Collections.emptyList(), rowTypeInfo, hiveTablePartition, MetaStoreUtils.getTableMetadata(hiveTable), false);
		outputFormat.open(0, 1);
		List<Row> toWrite = generateRecords();
		for (Row row : toWrite) {
			outputFormat.writeRecord(row);
		}
		outputFormat.close();
		outputFormat.finalizeGlobal(1);

		// verify written data
		Path outputFile = new Path(hiveTable.getSd().getLocation(), "0");
		FileSystem fs = outputFile.getFileSystem(jobConf);
		assertTrue(fs.exists(outputFile));
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
			int numWritten = 0;
			String line = reader.readLine();
			while (line != null) {
				assertEquals(toWrite.get(numWritten++).toString(), line.replaceAll("\u0001", ","));
				line = reader.readLine();
			}
			reader.close();
			assertEquals(toWrite.size(), numWritten);
		}
	}

	private List<Row> generateRecords() {
		int numRecords = 5;
		int arity = 4;
		List<Row> res = new ArrayList<>(numRecords);
		for (int i = 0; i < numRecords; i++) {
			Row row = new Row(arity);
			row.setField(0, i);
			row.setField(1, (long) i);
			row.setField(2, Double.valueOf(String.valueOf(String.format("%d.%d", i, i))));
			row.setField(3, String.valueOf((char) ('a' + i)));
			res.add(row);
		}
		return res;
	}
}
