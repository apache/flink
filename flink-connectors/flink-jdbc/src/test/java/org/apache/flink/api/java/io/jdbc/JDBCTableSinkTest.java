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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test for JDBCTableSink.
 */
public class JDBCTableSinkTest {
	private static final String[] FIELD_NAMES = new String[]{"foo"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
		BasicTypeInfo.STRING_TYPE_INFO
	};

	@Test
	public void testOutputSink() throws Exception {
		JDBCOutputFormat outputFormat = mock(JDBCOutputFormat.class);
		JDBCTableSink sink = new JDBCTableSink(outputFormat);
		@SuppressWarnings("unchecked")
		DataStream<Row> dataStream = (DataStream<Row>) mock(DataStream.class);
		sink.emitDataStream(dataStream);
		verify(dataStream).addSink(sink);
	}

	@Test
	public void testFlush() throws Exception {
		JDBCOutputFormat outputFormat = mock(JDBCOutputFormat.class);
		JDBCTableSink sink = new JDBCTableSink(outputFormat);
		@SuppressWarnings("unchecked")
		DataStream<Row> dataStream = (DataStream<Row>) mock(DataStream.class);
		sink.emitDataStream(dataStream);
		sink.snapshotState(mock(FunctionSnapshotContext.class));
		verify(dataStream).addSink(sink);
		verify(outputFormat).flush();
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
}
