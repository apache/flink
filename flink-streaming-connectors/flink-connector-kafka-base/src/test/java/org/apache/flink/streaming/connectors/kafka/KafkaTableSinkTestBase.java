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
package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.internals.TypeUtil;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.junit.Test;

import java.io.Serializable;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public abstract class KafkaTableSinkTestBase implements Serializable {

	protected final static String TOPIC = "testTopic";
	protected final static String[] FIELD_NAMES = new String[] {"field1", "field2"};
	protected final static TypeInformation[] FIELD_TYPES = TypeUtil.toTypeInfo(new Class[] {Integer.class, String.class});

	protected FlinkKafkaProducerBase<Row> kafkaProducer = mock(FlinkKafkaProducerBase.class);

	@Test
	public void testKafkaTableSink() throws Exception {
		DataStream dataStream = mock(DataStream.class);
		KafkaTableSink kafkaTableSink = createTableSink();
		kafkaTableSink.emitDataStream(dataStream);

		verify(dataStream).addSink(kafkaProducer);
	}

	@Test
	public void testConfigure() {
		KafkaTableSink kafkaTableSink = createTableSink();
		KafkaTableSink newKafkaTableSink = kafkaTableSink.configure(FIELD_NAMES, FIELD_TYPES);
		assertNotSame(kafkaTableSink, newKafkaTableSink);

		assertArrayEquals(FIELD_NAMES, newKafkaTableSink.getFieldNames());
		assertArrayEquals(FIELD_TYPES, newKafkaTableSink.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES), newKafkaTableSink.getOutputType());
	}

	protected KafkaPartitioner<Row> createPartitioner() {
		return new CustomPartitioner();
	}

	protected Properties createSinkProperties() {
		return new Properties();
	}

	protected abstract KafkaTableSink createTableSink();

	private static class CustomPartitioner extends KafkaPartitioner<Row> implements Serializable {
		@Override
		public int partition(Row next, byte[] serializedKey, byte[] serializedValue, int numPartitions) {
			return 0;
		}
	}
}
