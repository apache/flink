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
package org.apache.flink.streaming.connectors.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

/**
 * Immutable configuration for AMQSink
 * @param <IN> type of input messages in configured sink
 */
public class AMQSinkConfig<IN> {
	private final ActiveMQConnectionFactory connectionFactory;
	private final String queueName;
	private final SerializationSchema<IN> serializationSchema;
	private final boolean persistentDelivery;
	private final DestinationType destinationType;

	public AMQSinkConfig(ActiveMQConnectionFactory connectionFactory, String queueName,
						SerializationSchema<IN> serializationSchema, boolean persistentDelivery,
						DestinationType destinationType) {
		this.connectionFactory = Preconditions.checkNotNull(connectionFactory, "connectionFactory");
		this.queueName = Preconditions.checkNotNull(queueName, "destinationName");
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
		this.persistentDelivery = persistentDelivery;
		this.destinationType = Preconditions.checkNotNull(destinationType, "destinationType");
	}

	public ActiveMQConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public String getDestinationName() {
		return queueName;
	}

	public SerializationSchema<IN> getSerializationSchema() {
		return serializationSchema;
	}

	public boolean isPersistentDelivery() {
		return persistentDelivery;
	}

	public DestinationType getDestinationType() {
		return destinationType;
	}


	/**
	 * Builder for {@link AMQSinkConfig}
	 * @param <IN> type of input messages in configured sink
	 */
	public static class AMQSinkConfigBuilder<IN> {
		private ActiveMQConnectionFactory connectionFactory;
		private String destinationName;
		private SerializationSchema<IN> serializationSchema;
		private boolean persistentDelivery;
		private DestinationType destinationType = DestinationType.QUEUE;

		public AMQSinkConfigBuilder<IN> setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
			this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
			return this;
		}

		public AMQSinkConfigBuilder<IN> setDestinationName(String queueName) {
			this.destinationName = Preconditions.checkNotNull(queueName);
			return this;
		}

		public AMQSinkConfigBuilder<IN> setSerializationSchema(SerializationSchema<IN> serializationSchema) {
			this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
			return this;
		}

		public AMQSinkConfigBuilder<IN> setPersistentDelivery(boolean persistentDelivery) {
			this.persistentDelivery = persistentDelivery;
			return this;
		}

		public AMQSinkConfigBuilder<IN> setDestinationType(DestinationType destinationType) {
			this.destinationType = Preconditions.checkNotNull(destinationType);
			return this;
		}

		public AMQSinkConfig<IN> build() {
			return new AMQSinkConfig<IN>(connectionFactory, destinationName, serializationSchema, persistentDelivery, destinationType);
		}

	}
}
