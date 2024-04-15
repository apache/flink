/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MapStateDescriptor}. */
class MapStateDescriptorTest {

    @Test
    void testMapStateDescriptor() throws Exception {

        TypeSerializer<Integer> keySerializer =
                new KryoSerializer<>(Integer.class, new SerializerConfigImpl());
        TypeSerializer<String> valueSerializer =
                new KryoSerializer<>(String.class, new SerializerConfigImpl());

        MapStateDescriptor<Integer, String> descr =
                new MapStateDescriptor<>("testName", keySerializer, valueSerializer);

        assertThat(descr.getName()).isEqualTo("testName");
        assertThat(descr.getSerializer()).isNotNull();
        assertThat(descr.getSerializer()).isInstanceOf(MapSerializer.class);
        assertThat(descr.getKeySerializer()).isNotNull();
        assertThat(descr.getKeySerializer()).isEqualTo(keySerializer);
        assertThat(descr.getValueSerializer()).isNotNull();
        assertThat(descr.getValueSerializer()).isEqualTo(valueSerializer);

        MapStateDescriptor<Integer, String> copy = CommonTestUtils.createCopySerializable(descr);

        assertThat(copy.getName()).isEqualTo("testName");
        assertThat(copy.getSerializer()).isNotNull();
        assertThat(copy.getSerializer()).isInstanceOf(MapSerializer.class);

        assertThat(copy.getKeySerializer()).isNotNull();
        assertThat(copy.getKeySerializer()).isEqualTo(keySerializer);
        assertThat(copy.getValueSerializer()).isNotNull();
        assertThat(copy.getValueSerializer()).isEqualTo(valueSerializer);
    }

    @Test
    void testHashCodeEquals() throws Exception {
        final String name = "testName";

        MapStateDescriptor<String, String> original =
                new MapStateDescriptor<>(name, String.class, String.class);
        MapStateDescriptor<String, String> same =
                new MapStateDescriptor<>(name, String.class, String.class);
        MapStateDescriptor<String, String> sameBySerializer =
                new MapStateDescriptor<>(
                        name, StringSerializer.INSTANCE, StringSerializer.INSTANCE);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertThat(same).hasSameHashCodeAs(original);
        assertThat(sameBySerializer).hasSameHashCodeAs(original);

        assertThat(same).isEqualTo(original);
        assertThat(sameBySerializer).isEqualTo(original);

        // equality with a clone
        MapStateDescriptor<String, String> clone = CommonTestUtils.createCopySerializable(original);
        assertThat(clone).isEqualTo(original);

        // equality with an initialized
        clone.initializeSerializerUnlessSet(new ExecutionConfig());
        assertThat(clone).isEqualTo(original);

        original.initializeSerializerUnlessSet(new ExecutionConfig());
        assertThat(same).isEqualTo(original);
    }

    /**
     * FLINK-6775.
     *
     * <p>Tests that the returned serializer is duplicated. This allows to share the state
     * descriptor.
     */
    @Test
    void testSerializerDuplication() {
        // we need a serializer that actually duplicates for testing (a stateful one)
        // we use Kryo here, because it meets these conditions
        TypeSerializer<String> keySerializer =
                new KryoSerializer<>(String.class, new SerializerConfigImpl());
        TypeSerializer<Long> valueSerializer =
                new KryoSerializer<>(Long.class, new SerializerConfigImpl());

        MapStateDescriptor<String, Long> descr =
                new MapStateDescriptor<>("foobar", keySerializer, valueSerializer);

        TypeSerializer<String> keySerializerA = descr.getKeySerializer();
        TypeSerializer<String> keySerializerB = descr.getKeySerializer();
        TypeSerializer<Long> valueSerializerA = descr.getValueSerializer();
        TypeSerializer<Long> valueSerializerB = descr.getValueSerializer();

        // check that we did not retrieve the same serializers
        assertThat(keySerializerB).isNotSameAs(keySerializerA);
        assertThat(valueSerializerB).isNotSameAs(valueSerializerA);

        TypeSerializer<Map<String, Long>> serializerA = descr.getSerializer();
        TypeSerializer<Map<String, Long>> serializerB = descr.getSerializer();

        assertThat(serializerB).isNotSameAs(serializerA);
    }
}
