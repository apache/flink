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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.util.TableProperties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for JDBCTableFactory.
 */
public class JDBCTableFactoryTest extends JDBCTestBase {

	private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private StreamTableEnvironment streamTEnv = TableEnvironment.getTableEnvironment(env);
	private BatchTableEnvironment batchTEnv = TableEnvironment.getBatchTableEnvironment(env);
	private JDBCTableFactory factory;
	private Map<String, String> propertyMap;

	@Before
	public void before() {
		TableProperties properties = new TableProperties();
		properties.property(ConnectorDescriptorValidator.CONNECTOR_TYPE, "JDBC");
		properties.property("drivername", JDBCTestBase.DRIVER_CLASS);
		properties.property("dburl", DB_URL);
		properties.property("tablename", JDBCTestBase.OUTPUT_TABLE);
		Map<String, String> findPropertyMap = properties.toMap();

		properties.putSchemaIntoProperties(new RichTableSchema(
				new String[] {"id", "title", "author", "price", "qty"},
				new InternalType[] {DataTypes.INT, DataTypes.STRING,
						DataTypes.STRING, DataTypes.DOUBLE, DataTypes.INT}));
		propertyMap = properties.toMap();

		factory = TableFactoryService.find(JDBCTableFactory.class, findPropertyMap);
		env.setParallelism(1);
		TypeInformation.of(JDBCTestBase.TestEntry.class);
		batchTEnv.registerCollection(
				"sTable", Arrays.asList(TEST_DATA), "id, title, author, price, qty");
		streamTEnv.registerTable("sTable",
				streamTEnv.fromDataStream(env.fromCollection(Arrays.asList(TEST_DATA)),
						"id, title, author, price, qty"));
	}

	private void checkResult() throws SQLException {
		try (
				Connection dbConn = DriverManager.getConnection(DB_URL);
				PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
				ResultSet resultSet = statement.executeQuery()
		) {
			int recordCount = 0;
			while (resultSet.next()) {
				assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
				assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
				assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
				assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
				assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

				recordCount++;
			}
			assertEquals(TEST_DATA.length, recordCount);
		}
	}

	@Test
	public void testStreamAppendTableSink() throws IOException, SQLException {
		streamTEnv.sqlQuery("select id, title, author, price, qty from sTable")
				.writeToSink(factory.createStreamTableSink(propertyMap));
		streamTEnv.execute();
		checkResult();
	}

	@Test
	public void testBatchAppendTableSink() throws IOException, SQLException {
		batchTEnv.sqlQuery("select id, title, author, price, qty from sTable")
				.writeToSink(factory.createBatchTableSink(propertyMap));
		batchTEnv.execute();
		checkResult();
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
				Connection conn = DriverManager.getConnection(DB_URL);
				Statement stat = conn.createStatement()) {
			stat.execute("DELETE FROM " + OUTPUT_TABLE);

			stat.close();
			conn.close();
		}
	}

}
