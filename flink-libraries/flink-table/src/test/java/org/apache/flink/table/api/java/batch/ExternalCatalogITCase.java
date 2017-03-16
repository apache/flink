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

package org.apache.flink.table.api.java.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase;
import org.apache.flink.table.utils.CommonTestData;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class ExternalCatalogITCase extends TableProgramsCollectionTestBase {

	public ExternalCatalogITCase(TableConfigMode configMode) {
		super(configMode);
	}

	@Test
	public void testSQL() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog());

		String sqlQuery = "SELECT * FROM test.db1.tb1 " +
				"UNION ALL " +
				"(SELECT d, e, g FROM test.db2.tb2 WHERE d < 3)";

		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"
				+ "1,1,Hallo\n" + "2,2,Hallo Welt\n" + "2,3,Hallo Welt wie\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testTableAPI() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog());

		Table table1 = tableEnv.scan("test", "db1", "tb1");
		Table table2 = tableEnv.scan("test", "db2", "tb2");
		Table result = table2.where("d < 3").select("d, e, g").union(table1);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"
				+ "1,1,Hallo\n" + "2,2,Hallo Welt\n" + "2,3,Hallo Welt wie\n";
		compareResultAsText(results, expected);
	}
}
