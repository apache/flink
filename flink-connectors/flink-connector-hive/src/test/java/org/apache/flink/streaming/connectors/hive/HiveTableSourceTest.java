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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test.
 * todo: complete e2e test case and test joining, projection and other case.
 */
@Ignore
public class HiveTableSourceTest {

	@Test
	public void testScanWithHcataLog() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://"));
		tEnv.setDefaultDatabase("myHive", "default");
		tEnv.sqlQuery("select * from products").print();
	}

	@Test
	public void testScanPartitionTable() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://"));
		tEnv.setDefaultDatabase("myHive", "default");
		tEnv.sqlQuery("select * from pt_area_products").print();
	}

	@Test
	public void testScanPartitionTableWithPartitionPrune() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://"));
		tEnv.setDefaultDatabase("myHive", "default");
		Table table = tEnv.sqlQuery("select * from pt_area_products where ds = '2018-12-25'");
		table.print();
	}
}
