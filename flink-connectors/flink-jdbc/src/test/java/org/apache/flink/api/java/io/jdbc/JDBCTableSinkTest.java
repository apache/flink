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
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JDBCTableSinkTest {
	private static final String[] FIELD_NAMES = new String[] {"foo"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[] {
		BasicTypeInfo.STRING_TYPE_INFO
	};
	private static final TypeSerializer<Row> SERIALIZER = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES)
		.createSerializer(new ExecutionConfig());

	@Test
	public void testJDBCTableSink() throws Exception {
		JDBCOutputFormat outputFormat = mock(JDBCOutputFormat.class);
		CheckpointCommitter committer = mock(CheckpointCommitter.class);
		JDBCTableSink sink = new JDBCTableSink(committer, SERIALIZER, outputFormat, FIELD_NAMES, FIELD_TYPES);
		@SuppressWarnings("unchecked")
		DataStream<Row> dataStream = (DataStream<Row>) mock(DataStream.class);
		sink.emitDataStream(dataStream);
		verify(dataStream).transform(anyString(), Mockito.<TypeInformation<Row>> any(), same(sink));
	}

	@Test
	public void testConfiguration() throws Exception {
		JDBCOutputFormat outputFormat = mock(JDBCOutputFormat.class);
		CheckpointCommitter committer = mock(CheckpointCommitter.class);
		JDBCTableSink sink = new JDBCTableSink(committer, SERIALIZER, outputFormat, FIELD_NAMES, FIELD_TYPES);
		JDBCTableSink copied = (JDBCTableSink) sink.configure(
			sink.getFieldNames(), sink.getFieldTypes());
		assertNotSame(sink, copied);

		assertArrayEquals(FIELD_NAMES, copied.getFieldNames());
		assertArrayEquals(FIELD_TYPES, copied.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES, FIELD_NAMES), copied.getOutputType());
	}
}
