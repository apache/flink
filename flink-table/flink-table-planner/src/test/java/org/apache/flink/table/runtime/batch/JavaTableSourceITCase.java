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

package org.apache.flink.table.runtime.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.runtime.utils.CommonTestData;
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Integration tests for {@link BatchTableSource}.
 */
@RunWith(Parameterized.class)
public class JavaTableSourceITCase extends TableProgramsCollectionTestBase {

	public JavaTableSourceITCase(TableConfigMode configMode) {
		super(configMode);
	}

	@Test
	public void testBatchTableSourceTableAPI() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env, config());
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table result = tableEnv.scan("persons")
			.select("id, first, last, score");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "1,Mike,Smith,12.3\n" +
			"2,Bob,Taylor,45.6\n" +
			"3,Sam,Miller,7.89\n" +
			"4,Peter,Smith,0.12\n" +
			"5,Liz,Williams,34.5\n" +
			"6,Sally,Miller,6.78\n" +
			"7,Alice,Smith,90.1\n" +
			"8,Kelly,Williams,2.34\n";

		compareResultAsText(results, expected);
	}

	@Test
	public void testBatchTableSourceSQL() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env, config());
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table result = tableEnv
			.sqlQuery("SELECT `last`, FLOOR(id), score * 2 FROM persons WHERE score < 20");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "Smith,1,24.6\n" +
			"Miller,3,15.78\n" +
			"Smith,4,0.24\n" +
			"Miller,6,13.56\n" +
			"Williams,8,4.68\n";

		compareResultAsText(results, expected);
	}
}
