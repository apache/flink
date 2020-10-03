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

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface for the set of methods required to parse an RMQ delivery.
 * @param <T> The output type of the {@link RMQSource}
 */
public abstract class RMQDeserializationSchema<T> implements  Serializable, ResultTypeQueryable<T> {

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	public void open(DeserializationSchema.InitializationContext context) throws Exception {

	}

	/**
	 * This method takes all the RabbitMQ delivery information supplied by the client extract the data and pass it to the
	 * collector.
	 *
	 * <p><b>NOTICE:</b> The implementation of this method can call {@link RMQCollector#setMessageIdentifiers} with
	 * a custom correlation ID and delivery tag if checkpointing and UseCorrelationID (in the RMQSource constructor) were
	 * enabled
	 * the {@link RMQSource}.
	 * @param envelope an AMQP {@link Envelope}.
	 * @param properties the {@link AMQP.BasicProperties} of the message.
	 * @param body the message itself as a byte array.
	 * @param collector the {@link RMQCollector} that will collect the data.
	 * @throws IOException
	 */
	public abstract void deserialize(Envelope envelope, AMQP.BasicProperties properties, byte[] body, RMQCollector collector) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	abstract boolean isEndOfStream(T nextElement);

	/**
	 * Special collector for RMQ messages.
	 * Captures the correlation ID and delivery tag also does the filtering logic for weather a message has been
	 * processed or not.
	 * @param <T>
	 */
	public interface RMQCollector<T> extends Collector<T> {
		public void collect(List<T> records);

		public void setMessageIdentifiers(String correlationId, long deliveryTag);
	}
}
