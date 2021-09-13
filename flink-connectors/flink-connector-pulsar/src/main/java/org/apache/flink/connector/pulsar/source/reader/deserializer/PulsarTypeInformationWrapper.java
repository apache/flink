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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

/**
 * Wrap the flink TypeInformation into a {@code PulsarDeserializationSchema}. We would create a
 * flink {@code TypeSerializer} by using given ExecutionConfig. This execution config could be
 * {@link ExecutionEnvironment#getConfig()}.
 */
public class PulsarTypeInformationWrapper<T> implements PulsarDeserializationSchema<T> {
    private static final long serialVersionUID = 6647084180084963022L;

    /**
     * PulsarDeserializationSchema would be shared for multiple SplitReaders in different fetcher
     * thread. Use a thread-local DataInputDeserializer would be better.
     */
    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<DataInputDeserializer> DESERIALIZER =
            ThreadLocal.withInitial(DataInputDeserializer::new);

    private final TypeInformation<T> information;
    private final TypeSerializer<T> serializer;

    public PulsarTypeInformationWrapper(TypeInformation<T> information, ExecutionConfig config) {
        this.information = information;
        this.serializer = information.createSerializer(config);
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<T> out) throws Exception {
        DataInputDeserializer dis = DESERIALIZER.get();
        dis.setBuffer(message.getData());
        T instance = serializer.deserialize(dis);

        out.collect(instance);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return information;
    }
}
