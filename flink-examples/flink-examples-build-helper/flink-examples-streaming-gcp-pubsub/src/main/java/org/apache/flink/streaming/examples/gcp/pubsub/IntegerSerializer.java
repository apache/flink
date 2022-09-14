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

package org.apache.flink.streaming.examples.gcp.pubsub;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Deserialization schema to deserialize messages produced by {@link PubSubPublisher}. The byte[]
 * received by this schema must contain a single Integer.
 */
class IntegerSerializer
        implements PubSubDeserializationSchema<Integer>, SerializationSchema<Integer> {

    @Override
    public Integer deserialize(PubsubMessage message) throws IOException {
        return new BigInteger(message.getData().toByteArray()).intValue();
    }

    @Override
    public boolean isEndOfStream(Integer integer) {
        return false;
    }

    @Override
    public TypeInformation<Integer> getProducedType() {
        return TypeInformation.of(Integer.class);
    }

    @Override
    public byte[] serialize(Integer integer) {
        return BigInteger.valueOf(integer).toByteArray();
    }
}
