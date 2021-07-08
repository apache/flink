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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * A serialization and deserialization schema for Key Value Pairs that uses Flink's serialization
 * stack to transform typed from and to byte arrays.
 *
 * @param <K> The key type to be serialized.
 * @param <V> The value type to be serialized.
 */
@PublicEvolving
public class TypeInformationKeyValueSerializationSchema<K, V>
        implements KafkaDeserializationSchema<Tuple2<K, V>>,
                KeyedSerializationSchema<Tuple2<K, V>> {

    private static final long serialVersionUID = -5359448468131559102L;

    /** The serializer for the key. */
    private final TypeSerializer<K> keySerializer;

    /** The serializer for the value. */
    private final TypeSerializer<V> valueSerializer;

    /** reusable input deserialization buffer. */
    private final DataInputDeserializer inputDeserializer;

    /** reusable output serialization buffer for the key. */
    private transient DataOutputSerializer keyOutputSerializer;

    /** reusable output serialization buffer for the value. */
    private transient DataOutputSerializer valueOutputSerializer;

    /**
     * The type information, to be returned by {@link #getProducedType()}. It is transient, because
     * it is not serializable. Note that this means that the type information is not available at
     * runtime, but only prior to the first serialization / deserialization
     */
    private final transient TypeInformation<Tuple2<K, V>> typeInfo;

    // ------------------------------------------------------------------------

    /**
     * Creates a new de-/serialization schema for the given types.
     *
     * @param keyTypeInfo The type information for the key type de-/serialized by this schema.
     * @param valueTypeInfo The type information for the value type de-/serialized by this schema.
     * @param ec The execution config, which is used to parametrize the type serializers.
     */
    public TypeInformationKeyValueSerializationSchema(
            TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo, ExecutionConfig ec) {
        this.typeInfo = new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo);
        this.keySerializer = keyTypeInfo.createSerializer(ec);
        this.valueSerializer = valueTypeInfo.createSerializer(ec);
        this.inputDeserializer = new DataInputDeserializer();
    }

    /**
     * Creates a new de-/serialization schema for the given types. This constructor accepts the
     * types as classes and internally constructs the type information from the classes.
     *
     * <p>If the types are parametrized and cannot be fully defined via classes, use the constructor
     * that accepts {@link TypeInformation} instead.
     *
     * @param keyClass The class of the key de-/serialized by this schema.
     * @param valueClass The class of the value de-/serialized by this schema.
     * @param config The execution config, which is used to parametrize the type serializers.
     */
    public TypeInformationKeyValueSerializationSchema(
            Class<K> keyClass, Class<V> valueClass, ExecutionConfig config) {
        this(
                TypeExtractor.createTypeInfo(keyClass),
                TypeExtractor.createTypeInfo(valueClass),
                config);
    }

    // ------------------------------------------------------------------------

    @Override
    public Tuple2<K, V> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        K key = null;
        V value = null;

        if (record.key() != null) {
            inputDeserializer.setBuffer(record.key());
            key = keySerializer.deserialize(inputDeserializer);
        }
        if (record.value() != null) {
            inputDeserializer.setBuffer(record.value());
            value = valueSerializer.deserialize(inputDeserializer);
        }
        return new Tuple2<>(key, value);
    }

    /**
     * This schema never considers an element to signal end-of-stream, so this method returns always
     * false.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return Returns false.
     */
    @Override
    public boolean isEndOfStream(Tuple2<K, V> nextElement) {
        return false;
    }

    @Override
    public byte[] serializeKey(Tuple2<K, V> element) {
        if (element.f0 == null) {
            return null;
        } else {
            // key is not null. serialize it:
            if (keyOutputSerializer == null) {
                keyOutputSerializer = new DataOutputSerializer(16);
            }
            try {
                keySerializer.serialize(element.f0, keyOutputSerializer);
            } catch (IOException e) {
                throw new RuntimeException("Unable to serialize record", e);
            }
            // check if key byte array size changed
            byte[] res = keyOutputSerializer.getByteArray();
            if (res.length != keyOutputSerializer.length()) {
                byte[] n = new byte[keyOutputSerializer.length()];
                System.arraycopy(res, 0, n, 0, keyOutputSerializer.length());
                res = n;
            }
            keyOutputSerializer.clear();
            return res;
        }
    }

    @Override
    public byte[] serializeValue(Tuple2<K, V> element) {
        // if the value is null, its serialized value is null as well.
        if (element.f1 == null) {
            return null;
        }

        if (valueOutputSerializer == null) {
            valueOutputSerializer = new DataOutputSerializer(16);
        }

        try {
            valueSerializer.serialize(element.f1, valueOutputSerializer);
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize record", e);
        }

        byte[] res = valueOutputSerializer.getByteArray();
        if (res.length != valueOutputSerializer.length()) {
            byte[] n = new byte[valueOutputSerializer.length()];
            System.arraycopy(res, 0, n, 0, valueOutputSerializer.length());
            res = n;
        }
        valueOutputSerializer.clear();
        return res;
    }

    @Override
    public String getTargetTopic(Tuple2<K, V> element) {
        return null; // we are never overriding the topic
    }

    @Override
    public TypeInformation<Tuple2<K, V>> getProducedType() {
        if (typeInfo != null) {
            return typeInfo;
        } else {
            throw new IllegalStateException(
                    "The type information is not available after this class has been serialized and distributed.");
        }
    }
}
