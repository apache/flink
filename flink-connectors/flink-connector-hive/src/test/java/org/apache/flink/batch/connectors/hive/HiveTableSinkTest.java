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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogTable;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.runtime.utils.BatchTableEnvUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import scala.collection.JavaConversions;

/**
 * Tests {@link HiveTableSink}.
 */
public class HiveTableSinkTest {

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
			new String[]{"a", "b", "c", "d", "e"},
			new TypeInformation[]{
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO}
		);
		HiveCatalogTable catalogTable = new HiveCatalogTable(tableSchema, new HashMap<>(), "");
		hiveCatalog.createTable(tablePath, catalogTable, false);
		BatchTableEnvironment tableEnv = createTableEnv(1);
		Table table = getSmall5TupleDataSet(tableEnv);
		HiveTableSink hiveTableSink = new HiveTableSink(new JobConf(hiveConf),
			new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames()), dbName, tblName, Collections.emptyList());
		tableEnv.writeToSink(table, hiveTableSink, null);
		tableEnv.execute();
		// TODO: verify data is written properly
	}

	private BatchTableEnvironment createTableEnv(int parallelism) {
		Configuration config = new Configuration();
		config.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, config);
		return BatchTableEnvironment.create(env);
	}

	private Table getSmall5TupleDataSet(BatchTableEnvironment env) {
		List<Tuple5<Integer, Integer, String, Long, Double>> data = new ArrayList();
		data.add(new Tuple5<>(1, 10, "Hi", 11L, 1.11));
		data.add(new Tuple5<>(2, 20, "Hello", 22L, 2.22));
		data.add(new Tuple5<>(3, 30, "Hello world!", 33L, 3.33));

		return BatchTableEnvUtil.fromCollection(env, JavaConversions.collectionAsScalaIterable(data),
			TypeInformation.of(new TypeHint<Tuple5<Integer, Integer, String, Long, Double>>() {
			}),
			"a,b,c,d,e");
	}
}
