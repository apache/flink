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

package org.apache.flink.api.java.batch;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.scala.batch.GeneratingInputFormat;
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sources.BatchTableSource;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class TableSourceITCase extends TableProgramsTestBase {

	public TableSourceITCase(TestExecutionMode mode, TableConfigMode configMode) {
		super(mode, configMode);
	}

	@Test
	public void testBatchTableSourceTableAPI() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		tableEnv.registerTableSource("MyTable", new TestBatchTableSource());

		Table result = tableEnv.scan("MyTable")
			.where("amount < 4")
			.select("amount * id, name");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "0,Record_0\n" + "0,Record_16\n" + "0,Record_32\n" + "1,Record_1\n" +
			"17,Record_17\n" + "36,Record_18\n" + "4,Record_2\n" + "57,Record_19\n" + "9,Record_3\n";

		compareResultAsText(results, expected);
	}

	@Test
	public void testBatchTableSourceSQL() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		tableEnv.registerTableSource("MyTable", new TestBatchTableSource());

		Table result = tableEnv
			.sql("SELECT amount * id, name FROM MyTable WHERE amount < 4");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "0,Record_0\n" + "0,Record_16\n" + "0,Record_32\n" + "1,Record_1\n" +
			"17,Record_17\n" + "36,Record_18\n" + "4,Record_2\n" + "57,Record_19\n" + "9,Record_3\n";

		compareResultAsText(results, expected);
	}

	public static class TestBatchTableSource implements BatchTableSource<Row> {

		private TypeInformation[] fieldTypes = new TypeInformation<?>[] {
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};

		@Override
		public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
			return execEnv.createInput(new GeneratingInputFormat(33), getReturnType()).setParallelism(1);
		}

		@Override
		public int getNumberOfFields() {
			return 3;
		}

		@Override
		public String[] getFieldsNames() {
			return new String[]{"name", "id", "amount"};
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return fieldTypes;
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			return new RowTypeInfo(fieldTypes);
		}
	}

}
