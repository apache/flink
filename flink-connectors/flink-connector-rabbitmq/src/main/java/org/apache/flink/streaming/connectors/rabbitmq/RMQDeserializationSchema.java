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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for the set of methods required to parse an RMQ delivery.
 * @param <T> The output type of the {@link RMQSource}
 */
@PublicEvolving
public interface RMQDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features
	 * such as e.g. registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	default void open(DeserializationSchema.InitializationContext context) throws Exception {
	}

	/**
	 * This method takes all the RabbitMQ delivery information supplied by the client extract the data and pass it to
	 * the collector.
	 *
	 * <p><b>NOTICE:</b> The implementation of this method can call {@link RMQCollector#setMessageIdentifiers} with
	 * a custom correlation ID and delivery tag if checkpointing and {@code useCorrelationID} were enabled in the
	 * constructor of {@link RMQSource}.
	 *
	 * @param envelope an AMQP {@link Envelope}.
	 * @param properties the {@link AMQP.BasicProperties} of the message.
	 * @param body the message itself as a byte array.
	 * @param collector the {@link RMQCollector} that will collect the data.
	 * @throws IOException When the body of the message can't be parsed
	 */
	void deserialize(
		Envelope envelope,
		AMQP.BasicProperties properties,
		byte[] body,
		RMQCollector<T> collector) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	boolean isEndOfStream(T nextElement);

	/**
	 * Special collector for RMQ messages.
	 *
	 * <p>It extends the {@link Collector} to give the ability to collect more than 1 message and the ability to set
	 * the message correlationId and deliveryTag.
	 */
	interface RMQCollector<T> extends Collector<T> {
		/**
		 * Sets the correlation id and the delivery tag that corresponds to the records originating from the
		 * RMQ event. If the correlation id has been processed before, records will not be emitted downstream.
		 *
		 * <p>If not set explicitly, the {@link AMQP.BasicProperties#getCorrelationId()} and
		 * {@link Envelope#getDeliveryTag()} will be used.
		 *
		 * <b>NOTE:</b>Can be called once for a single invokation of a
		 * {@link RMQDeserializationSchema#deserialize(Envelope, AMQP.BasicProperties, byte[], RMQCollector)} method.
		 *
		 * @return true, if a message with given correlationId was seen before
		 */
		boolean setMessageIdentifiers(String correlationId, long deliveryTag);
	}
}
