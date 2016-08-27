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
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.junit.Test;

import java.io.Serializable;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public abstract class KafkaTableSinkTestBase implements Serializable {

	private final static String TOPIC = "testTopic";
	private final static String[] FIELD_NAMES = new String[] {"field1", "field2"};
	private final static TypeInformation[] FIELD_TYPES = TypeUtil.toTypeInfo(new Class[] {Integer.class, String.class});

	private final KafkaPartitioner<Row> partitioner = new CustomPartitioner();
	private final Properties properties = createSinkProperties();
	@SuppressWarnings("unchecked")
	private final FlinkKafkaProducerBase<Row> kafkaProducer = mock(FlinkKafkaProducerBase.class);

	@Test
	@SuppressWarnings("unchecked")
	public void testKafkaTableSink() throws Exception {
		DataStream dataStream = mock(DataStream.class);
		KafkaTableSink kafkaTableSink = createTableSink();
		kafkaTableSink.emitDataStream(dataStream);

		verify(dataStream).addSink(kafkaProducer);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreatedProducer() throws Exception {
		DataStream dataStream = mock(DataStream.class);
		KafkaTableSink kafkaTableSink = spy(createTableSink());
		kafkaTableSink.emitDataStream(dataStream);

		verify(kafkaTableSink).createKafkaProducer(
			eq(TOPIC),
			eq(properties),
			any(getSerializationSchema()),
			eq(partitioner));
	}

	@Test
	public void testConfiguration() {
		KafkaTableSink kafkaTableSink = createTableSink();
		KafkaTableSink newKafkaTableSink = kafkaTableSink.configure(FIELD_NAMES, FIELD_TYPES);
		assertNotSame(kafkaTableSink, newKafkaTableSink);

		assertArrayEquals(FIELD_NAMES, newKafkaTableSink.getFieldNames());
		assertArrayEquals(FIELD_TYPES, newKafkaTableSink.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES), newKafkaTableSink.getOutputType());
	}

	protected abstract KafkaTableSink createTableSink(String topic, Properties properties,
			KafkaPartitioner<Row> partitioner, FlinkKafkaProducerBase<Row> kafkaProducer);

	protected abstract Class<SerializationSchema<Row>> getSerializationSchema();

	private KafkaTableSink createTableSink() {
		return createTableSink(TOPIC, properties, partitioner, kafkaProducer);
	}

	private static Properties createSinkProperties() {
		Properties properties = new Properties();
		properties.setProperty("testKey", "testValue");
		return properties;
	}

	private static class CustomPartitioner extends KafkaPartitioner<Row> implements Serializable {
		@Override
		public int partition(Row next, byte[] serializedKey, byte[] serializedValue, int numPartitions) {
			return 0;
		}
	}
}
