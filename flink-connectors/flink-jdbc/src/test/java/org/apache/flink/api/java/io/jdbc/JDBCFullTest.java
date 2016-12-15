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

package org.apache.flink.api.java.io.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class JDBCFullTest extends JDBCTestBase {

	@Test
	public void testJdbcInOut() throws Exception {
		//run without parallelism
		runTest(false);

		//cleanup
		JDBCTestBase.tearDownClass();
		JDBCTestBase.prepareTestDb();
		
		//run expliting parallelism
		runTest(true);
		
	}

	private void runTest(boolean exploitParallelism) {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS)
				.setRowTypeInfo(rowTypeInfo);

		if(exploitParallelism) {
			final int fetchSize = 1;
			final Long min = new Long(JDBCTestBase.testData[0][0].toString());
			final Long max = new Long(JDBCTestBase.testData[JDBCTestBase.testData.length - fetchSize][0].toString());
			//use a "splittable" query to exploit parallelism
			inputBuilder = inputBuilder
					.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
					.setParametersProvider(new NumericBetweenParametersProvider(fetchSize, min, max));
		}
		DataSet<Row> source = environment.createInput(inputBuilder.finish());

		//NOTE: in this case (with Derby driver) setSqlTypes could be skipped, but
		//some database, doens't handle correctly null values when no column type specified
		//in PreparedStatement.setObject (see its javadoc for more details)
		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
				.setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR,Types.DOUBLE,Types.INTEGER})
				.finish());
		try {
			environment.execute();
		} catch (Exception e) {
			Assert.fail("JDBC full test failed. " + e.getMessage());
		}

		try (
			Connection dbConn = DriverManager.getConnection(JDBCTestBase.DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
			ResultSet resultSet = statement.executeQuery()
		) {
			int count = 0;
			while (resultSet.next()) {
				count++;
			}
			Assert.assertEquals(JDBCTestBase.testData.length, count);
		} catch (SQLException e) {
			Assert.fail("JDBC full test failed. " + e.getMessage());
		}
	}

}
