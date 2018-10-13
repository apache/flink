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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Abstract test base for all Kafka table sink tests.
 *
 * @deprecated Ensures backwards compatibility with Flink 1.5. Can be removed once we
 *             drop support for format-specific table sinks.
 */
@Deprecated
public abstract class KafkaTableSinkBaseTestBase {

	private static final String TOPIC = "testTopic";
	private static final String[] FIELD_NAMES = new String[] {"field1", "field2"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[] { Types.INT(), Types.STRING() };
	private static final FlinkKafkaPartitioner<Row> PARTITIONER = new CustomPartitioner();
	private static final Properties PROPERTIES = createSinkProperties();

	@SuppressWarnings("unchecked")
	@Test
	public void testKafkaTableSink() {
		DataStream dataStream = mock(DataStream.class);
		when(dataStream.addSink(any(SinkFunction.class))).thenReturn(mock(DataStreamSink.class));

		KafkaTableSinkBase kafkaTableSink = spy(createTableSink());
		kafkaTableSink.emitDataStream(dataStream);

		// verify correct producer class
		verify(dataStream).addSink(any(getProducerClass()));

		// verify correctly configured producer
		verify(kafkaTableSink).createKafkaProducer(
			eq(TOPIC),
			eq(PROPERTIES),
			any(getSerializationSchemaClass()),
			eq(Optional.of(PARTITIONER)));
	}

	@Test
	public void testConfiguration() {
		KafkaTableSinkBase kafkaTableSink = createTableSink();
		KafkaTableSinkBase newKafkaTableSink = kafkaTableSink.configure(FIELD_NAMES, FIELD_TYPES);
		assertNotSame(kafkaTableSink, newKafkaTableSink);

		assertArrayEquals(FIELD_NAMES, newKafkaTableSink.getFieldNames());
		assertArrayEquals(FIELD_TYPES, newKafkaTableSink.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES), newKafkaTableSink.getOutputType());
	}

	protected abstract KafkaTableSinkBase createTableSink(
		String topic,
		Properties properties,
		FlinkKafkaPartitioner<Row> partitioner);

	protected abstract Class<? extends SerializationSchema<Row>> getSerializationSchemaClass();

	protected abstract Class<? extends FlinkKafkaProducerBase> getProducerClass();

	private KafkaTableSinkBase createTableSink() {
		KafkaTableSinkBase sink = createTableSink(TOPIC, PROPERTIES, PARTITIONER);
		return sink.configure(FIELD_NAMES, FIELD_TYPES);
	}

	private static Properties createSinkProperties() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:12345");
		return properties;
	}

	private static class CustomPartitioner extends FlinkKafkaPartitioner<Row> {
		@Override
		public int partition(Row record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
			return 0;
		}
	}
}
