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

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * The message computation provides methods to compute the message routing key and/or the
 * properties.
 *
 * @param <IN> The type of the data used by the sink.
 */
@PublicEvolving
public interface RMQSinkPublishOptions<IN> extends java.io.Serializable {

    /**
     * Compute the message's routing key from the data.
     *
     * @param a The data used by the sink
     * @return The routing key of the message null will raise a NullPointerException
     */
    String computeRoutingKey(IN a);

    /**
     * Compute the message's properties from the data.
     *
     * @param a The data used by the sink
     * @return The message's properties (can be null)
     */
    BasicProperties computeProperties(IN a);

    /**
     * Compute the exchange from the data.
     *
     * @param a The data used by the sink
     * @return The exchange to publish the message to null will raise a NullPointerException
     */
    String computeExchange(IN a);

    /**
     * Compute the mandatory flag passed to method {@link
     * com.rabbitmq.client.Channel#basicPublish(String, String, boolean, boolean, BasicProperties,
     * byte[])}. A {@link SerializableReturnListener} is mandatory if this flag can be true.
     *
     * @param a The data used by the sink
     * @return The mandatory flag
     */
    default boolean computeMandatory(IN a) {
        return false;
    }

    /**
     * Compute the immediate flag passed to method {@link
     * com.rabbitmq.client.Channel#basicPublish(String, String, boolean, boolean, BasicProperties,
     * byte[])}. A {@link SerializableReturnListener} is mandatory if this flag can be true.
     *
     * @param a The data used by the sink
     * @return The mandatory flag
     */
    default boolean computeImmediate(IN a) {
        return false;
    }
}
