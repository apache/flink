/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.rethinkdb;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.flink.configuration.Configuration;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import org.mockito.runners.MockitoJUnitRunner;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;

@RunWith(MockitoJUnitRunner.class)
public class FlinkRethinkDbSinkTest {

	private static final String JSON_TEST_TABLE = "JsonTestTable";

	protected FlinkRethinkDbSink<String> sink;

	@Mock
	protected RethinkDB mockRethinkDB;

	@Mock
	protected Connection mockRethinkDBConnection;

	@Mock(answer=Answers.RETURNS_DEEP_STUBS)
	private Table mockRethinkDBTable;
	
	@Mock(answer=Answers.RETURNS_DEEP_STUBS)
	protected Insert mockInsert;

	@Mock(answer=Answers.RETURNS_DEEP_STUBS)
	private com.rethinkdb.net.Connection.Builder builder;
 	
	protected HashMap<String,Object> result = new HashMap<>();

	@Before
	public void setUp() {
		mockRethinkDBTable = mock(Table.class);
		sink = new FlinkRethinkDbSink<String>
			("localhost", 28015, "test", JSON_TEST_TABLE, new StringJSONSerializationSchema()){
				private static final long serialVersionUID = 1L;
				@Override
				protected RethinkDB getRethinkDB() {
					return mockRethinkDB;
				}
				@Override
				protected HashMap<String, Object> runInsert(Insert insert) {
					return result;
				}
		};
		when(mockRethinkDB.connection()).thenReturn(builder);
		when(builder.hostname("localhost").port(28015).user("admin", "").connect()).
			thenReturn(mockRethinkDBConnection);
	}	
	
	@Test
	public void testOpen() throws Exception {
		
		when(mockRethinkDB.table(Mockito.eq(JSON_TEST_TABLE))).thenReturn(mockRethinkDBTable);
		
		sink.open(new Configuration());
		
		Connection connection = sink.getRethinkDbConnection();
		Table table = sink.getRdbTable();
		
		assertEquals("Connection should be same", mockRethinkDBConnection, connection);
		assertEquals("Table should be same", mockRethinkDBTable, table);
	}

	@Test
	public void testClose() throws Exception {
		sink.open(new Configuration());
		sink.close();
		verify(mockRethinkDBConnection).close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInvokeSuccess() throws Exception {
		result.put(FlinkRethinkDbSink.RESULT_ERROR_KEY, 0L);
		when(mockRethinkDB.table(JSON_TEST_TABLE)).thenReturn(mockRethinkDBTable);
		
		JSONObject json = new JSONObject();
		json.put("key1", "value1");

		when(mockRethinkDBTable.insert(json)).thenReturn(mockInsert);
		when(mockInsert.optArg(FlinkRethinkDbSink.CONFLICT_OPT,
				ConflictStrategy.update.toString())).thenReturn(mockInsert);
		sink.open(new Configuration());
		sink.invoke(json.toString());
		
		verify(mockInsert).optArg(FlinkRethinkDbSink.CONFLICT_OPT,
				ConflictStrategy.update.toString());
		verify(mockRethinkDBTable).insert(json);
	}

	@SuppressWarnings("unchecked")
	@Test(expected=RuntimeException.class)
	public void testInvokeErrors() throws Exception {
		
		result.put(FlinkRethinkDbSink.RESULT_ERROR_KEY, 1L);
		when(mockRethinkDB.table(JSON_TEST_TABLE)).thenReturn(mockRethinkDBTable);
		
		JSONObject json = new JSONObject();
		json.put("key1", "value1");

		when(mockRethinkDBTable.insert(json)).thenReturn(mockInsert);
		when(mockInsert.optArg(FlinkRethinkDbSink.CONFLICT_OPT,
				ConflictStrategy.update.toString())).thenReturn(mockInsert);
		sink.open(new Configuration());
		
		try {
			sink.invoke(json.toString());
		}
		finally {
			verify(mockInsert).optArg(FlinkRethinkDbSink.CONFLICT_OPT,
					ConflictStrategy.update.toString());
			verify(mockInsert).optArg(FlinkRethinkDbSink.CONFLICT_OPT,
				ConflictStrategy.update.toString());
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNullUsername() throws Exception {
		sink.setUsernameAndPassword(null, "abcd");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testEmptyUsername() throws Exception {
		sink.setUsernameAndPassword("", "abcd");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNullPassword() throws Exception {
		sink.setUsernameAndPassword("user", null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testEmptyPassword() throws Exception {
		sink.setUsernameAndPassword("abcd", "");
	}
}
