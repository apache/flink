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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for JDBCTableSink.
 */
public class JDBCTableSinkTest extends JDBCTestBase {
	private static final String[] FIELD_NAMES = new String[]{"foo"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
		BasicTypeInfo.STRING_TYPE_INFO
	};


	private JDBCOutputFormat jdbcOutputFormat;

	@After
	public void tearDown() throws IOException {
		if (jdbcOutputFormat != null) {
			jdbcOutputFormat.close();
		}
		jdbcOutputFormat = null;
	}

	@Test
	public void testFlush() throws Exception {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
			.setBatchInterval(3)
			.finish();
		RuntimeContext ctx = mock(RuntimeContext.class);
		doReturn(0).when(ctx).getIndexOfThisSubtask();
		doReturn(1).when(ctx).getNumberOfParallelSubtasks();
		JDBCTableSink sink = new JDBCTableSink(jdbcOutputFormat);
		sink.setRuntimeContext(ctx);
		sink.open(new Configuration());
		try (
			Connection dbConn = DriverManager.getConnection(DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
		) {
			for (int i = 0; i < 2; ++i) {
				jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
			}
			try (ResultSet resultSet = statement.executeQuery()) {
				assertFalse(resultSet.next());
			}
			jdbcOutputFormat.writeRecord(toRow(TEST_DATA[2]));
			try (ResultSet resultSet = statement.executeQuery()) {
				int recordCount = 0;
				while (resultSet.next()) {
					assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
					assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
					assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
					assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
					assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));
					recordCount++;
				}
				assertEquals(3, recordCount);
			}
		} finally {
			sink.close();
		}
	}

	@Test
	public void testConfiguration() throws Exception {
		JDBCOutputFormat outputFormat = mock(JDBCOutputFormat.class);
		JDBCTableSink sink = new JDBCTableSink(outputFormat);
		JDBCTableSink copied = (JDBCTableSink) sink.configure(
			FIELD_NAMES, FIELD_TYPES);
		assertNotSame(sink, copied);

		assertArrayEquals(FIELD_NAMES, copied.getFieldNames());
		assertArrayEquals(FIELD_TYPES, copied.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES, FIELD_NAMES), copied.getOutputType());
	}

	private static Row toRow(TestEntry entry) {
		Row row = new Row(5);
		row.setField(0, entry.id);
		row.setField(1, entry.title);
		row.setField(2, entry.author);
		row.setField(3, entry.price);
		row.setField(4, entry.qty);
		return row;
	}
}
