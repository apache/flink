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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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
				.setFieldTypes(FIELD_TYPES)
				.build();

		StreamExecutionEnvironment env =
		mock(StreamExecutionEnvironment.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				return invocationOnMock.getArguments()[0];
			}
		}).when(env).clean(any());

		TypeSerializer<Row> ts = ROW_TYPE.createSerializer(mock(ExecutionConfig.class));
		FromElementsFunction<Row> func = new FromElementsFunction<>(ts, Row.of("foo"));
		DataStream<Row> ds = new DataStreamSource<>(env, ROW_TYPE, new StreamSource<>(func), false, "foo");
		DataStreamSink<Row> dsSink = ds.addSink(sink.getSink());
		assertSame(sink.getSink(), dsSink.getTransformation().getOperator().getUserFunction());
	}
}
