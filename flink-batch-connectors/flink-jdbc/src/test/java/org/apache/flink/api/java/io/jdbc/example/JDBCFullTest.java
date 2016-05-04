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

package org.apache.flink.api.java.io.jdbc.example;

import java.sql.Types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCTestBase;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.junit.Test;

public class JDBCFullTest extends JDBCTestBase {
	
	@Test
	public void test() throws Exception {
		//run without parallelism
		runTest(false);

		//cleanup
		JDBCTestBase.tearDownClass();
		JDBCTestBase.prepareTestDb();
		
		//run expliting parallelism
		runTest(true);
		
	}
	
	private void runTest(boolean exploitParallelism) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS);
		
		if(exploitParallelism) {
			final int fetchSize = 1;
			final Long min = new Long(JDBCTestBase.testData[0][0] + "");
			final Long max = new Long(JDBCTestBase.testData[JDBCTestBase.testData.length - fetchSize][0] + "");
			//use a "splittable" query to exploit parallelism
			inputBuilder = inputBuilder
					.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
					.setParametersProvider(new NumericBetweenParametersProvider(fetchSize, min, max));
		}
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};
		DataSet<Row> source = environment.createInput(inputBuilder.finish(), new RowTypeInfo(fieldTypes));
		//when rowTypeInfo is not passed the IF should always re-instantiated a row in nextRecord()
		//DataSet<Row> source = environment.createInput(inputBuilder.finish());

		//NOTE: in this case (with Derby driver) setSqlTypes could be skipped, but
		//some database, doens't handle correctly null values when no column type specified
		//in PreparedStatement.setObject (see its javadoc for more details)
		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(JDBCTestBase.DRIVER_CLASS)
			.setDBUrl(JDBCTestBase.DB_URL)
			.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
			.setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR,Types.DOUBLE,Types.INTEGER})
			.finish());
		environment.execute();
	}

}
