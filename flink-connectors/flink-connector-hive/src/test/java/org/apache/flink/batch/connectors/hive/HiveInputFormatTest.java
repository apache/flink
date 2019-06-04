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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Tests {@link HiveTableInputFormat}.
 */
public class HiveInputFormatTest {

	public static final String DEFAULT_HIVE_INPUT_FORMAT_TEST_SERDE_CLASS = org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName();
	public static final String DEFAULT_HIVE_INPUT_FORMAT_TEST_INPUT_FORMAT_CLASS = org.apache.hadoop.mapred.TextInputFormat.class.getName();
	public static final String DEFAULT_OUTPUT_FORMAT_CLASS = org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName();

	private static HiveCatalog hiveCatalog;
	private static HiveConf hiveConf;

	@BeforeClass
	public static void createCatalog() throws IOException {
		hiveConf = HiveTestUtils.createHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
	}

	@AfterClass
	public static void closeCatalog() {
		if (null != hiveCatalog) {
			hiveCatalog.close();
		}
	}

	@Test
	public void testReadFromHiveInputFormat() throws Exception {
		final String dbName = "default";
		final String tblName = "test";
		TableSchema tableSchema = new TableSchema(
				new String[]{"a", "b", "c", "d", "e"},
				new TypeInformation[]{
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO}
		);
		//Now we used metaStore client to create hive table instead of using hiveCatalog for it doesn't support set
		//serDe temporarily.
		HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(hiveConf);
		org.apache.hadoop.hive.metastore.api.Table tbl = new org.apache.hadoop.hive.metastore.api.Table();
		tbl.setDbName(dbName);
		tbl.setTableName(tblName);
		tbl.setCreateTime((int) (System.currentTimeMillis() / 1000));
		tbl.setParameters(new HashMap<>());
		StorageDescriptor sd = new StorageDescriptor();
		String location = HiveInputFormatTest.class.getResource("/test").getPath();
		sd.setLocation(location);
		sd.setInputFormat(DEFAULT_HIVE_INPUT_FORMAT_TEST_INPUT_FORMAT_CLASS);
		sd.setOutputFormat(DEFAULT_OUTPUT_FORMAT_CLASS);
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setSerializationLib(DEFAULT_HIVE_INPUT_FORMAT_TEST_SERDE_CLASS);
		sd.getSerdeInfo().setParameters(new HashMap<>());
		sd.getSerdeInfo().getParameters().put("serialization.format", "1");
		sd.getSerdeInfo().getParameters().put("field.delim", ",");
		sd.setCols(HiveTableUtil.createHiveColumns(tableSchema));
		tbl.setSd(sd);
		tbl.setPartitionKeys(new ArrayList<>());

		client.createTable(tbl);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
		List<HiveTablePartition> partitions = new ArrayList<>();
		partitions.add(new HiveTablePartition(sd, new HashMap<>()));
		HiveTableInputFormat hiveTableInputFormat = new HiveTableInputFormat(new JobConf(hiveConf), false, null,
																			partitions, rowTypeInfo);
		DataSet<Row> rowDataSet = env.createInput(hiveTableInputFormat);
		List<Row> rows = rowDataSet.collect();
		Assert.assertEquals(4, rows.size());
		Assert.assertEquals("1,1,a,1000,1.11", rows.get(0).toString());
		Assert.assertEquals("2,2,a,2000,2.22", rows.get(1).toString());
		Assert.assertEquals("3,3,a,3000,3.33", rows.get(2).toString());
		Assert.assertEquals("4,4,a,4000,4.44", rows.get(3).toString());
	}
}
