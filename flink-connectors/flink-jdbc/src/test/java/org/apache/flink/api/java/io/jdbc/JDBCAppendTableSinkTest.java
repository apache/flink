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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test for JDBCAppendTableSink.
 */
public class JDBCAppendTableSinkTest {
	private static final String[] FIELD_NAMES = new String[]{"foo"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
		BasicTypeInfo.STRING_TYPE_INFO
	};
	private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

	@Test
	public void testAppendTableSink() throws IOException {
		JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
			.setDrivername("foo")
			.setDBUrl("bar")
			.setQuery("insert into %s (id) values (?)")
			.setParameterTypes(FIELD_TYPES)
			.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Row> ds = env.fromCollection(Collections.singleton(Row.of("foo")), ROW_TYPE);
		sink.emitDataStream(ds);

		Collection<Integer> sinkIds = env.getStreamGraph().getSinkIDs();
		assertEquals(1, sinkIds.size());
		int sinkId = sinkIds.iterator().next();

		StreamSink planSink = (StreamSink) env.getStreamGraph().getStreamNode(sinkId).getOperator();
		assertTrue(planSink.getUserFunction() instanceof JDBCSinkFunction);

		JDBCSinkFunction sinkFunction = (JDBCSinkFunction) planSink.getUserFunction();
		assertSame(sink.getOutputFormat(), sinkFunction.outputFormat);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTypeCompatibilityCheck() throws IOException {

		JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
			.setDrivername("foo")
			.setDBUrl("bar")
			.setQuery("INSERT INTO foobar (id) VALUES (?)")
			.setParameterTypes(Types.LONG, Types.STRING, Types.INT)
			.build();

		sink.configure(
			new String[] {"Hello"},
			new TypeInformation<?>[] {Types.STRING, Types.INT, Types.LONG});
	}

}
