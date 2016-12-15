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

import java.util.Properties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public abstract class KafkaTableSourceTestBase {

	private static final String TOPIC = "testTopic";
	private static final String[] FIELD_NAMES = new String[] { "long", "string", "boolean", "double", "missing-field" };
	private static final TypeInformation<?>[] FIELD_TYPES = new TypeInformation<?>[] {
		BasicTypeInfo.LONG_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.BOOLEAN_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.LONG_TYPE_INFO };
	private static final Properties PROPERTIES = createSourceProperties();

	@Test
	public void testKafkaTableSource() {
		KafkaTableSource kafkaTableSource = spy(createTableSource());
		StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
		kafkaTableSource.getDataStream(env);

		verify(env).addSource(any(getFlinkKafkaConsumer()));

		verify(kafkaTableSource).getKafkaConsumer(
			eq(TOPIC),
			eq(PROPERTIES),
			any(getDeserializationSchema()));
	}

	protected abstract KafkaTableSource createTableSource(String topic, Properties properties,
			String[] fieldNames, TypeInformation<?>[] typeInfo);

	protected abstract Class<DeserializationSchema<Row>> getDeserializationSchema();

	protected abstract Class<FlinkKafkaConsumerBase<Row>> getFlinkKafkaConsumer();

	private KafkaTableSource createTableSource() {
		return createTableSource(TOPIC, PROPERTIES, FIELD_NAMES, FIELD_TYPES);
	}

	private static Properties createSourceProperties() {
		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "dummy");
		properties.setProperty("group.id", "dummy");
		return properties;
	}
}
