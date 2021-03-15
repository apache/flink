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

package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;

/**
 * The RabbitMQSourceReaderAtMostOnce provides at-most-once guarantee. Messages are automatically
 * acknowledged when received from RabbitMQ and afterwards consumed by the output. In case of a
 * failure in Flink messages might be lost.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQSourceReaderBase
 */
public class RabbitMQSourceReaderAtMostOnce<T> extends RabbitMQSourceReaderBase<T> {

    public RabbitMQSourceReaderAtMostOnce(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        super(sourceReaderContext, deliveryDeserializer);
    }

    @Override
    protected boolean isAutoAck() {
        return true;
    }
}
