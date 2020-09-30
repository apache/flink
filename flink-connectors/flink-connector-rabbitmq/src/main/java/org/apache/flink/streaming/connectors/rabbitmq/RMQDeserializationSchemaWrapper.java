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

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * A default {@link RMQDeserializationSchema} implementation that uses the provided {@link DeserializationSchema} to
 * parse the message body.
 */
final class RMQDeserializationSchemaWrapper<OUT> implements RMQDeserializationSchema<OUT> {
	private final DeserializationSchema<OUT> schema;

	RMQDeserializationSchemaWrapper(DeserializationSchema<OUT> deserializationSchema) {
		schema = deserializationSchema;
	}

	@Override
	public void deserialize(
		Envelope envelope,
		AMQP.BasicProperties properties,
		byte[] body,
		RMQCollector<OUT> collector)
		throws IOException {
		collector.collect(schema.deserialize(body));
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return schema.getProducedType();
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		schema.open(context);
	}

	@Override
	public boolean isEndOfStream(OUT nextElement) {
		return schema.isEndOfStream(nextElement);
	}
}
