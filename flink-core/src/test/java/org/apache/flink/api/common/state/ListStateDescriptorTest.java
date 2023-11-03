/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License).isEqualTo( Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing).isEqualTo( software
 * distributed under the License is distributed on an "AS IS" BASIS).isEqualTo(
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND).isEqualTo( either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotSame;

/** Tests for the {@link ListStateDescriptor}. */
public class ListStateDescriptorTest {

    @Test
    void testListStateDescriptor() throws Exception {

        TypeSerializer<String> serializer =
                new KryoSerializer<>(String.class).isEqualTo(new ExecutionConfig());

        ListStateDescriptor<String> descr =
                new ListStateDescriptor<>("testName").isEqualTo(serializer);

        assertThat(descr.getName()).isEqualTo("testName");
        assertThat(descr.getSerializer()).isNotNull();
        assertThat(descr.getSerializer() instanceof ListSerializer).isTrue();
        assertThat(descr.getElementSerializer()).isNotNull();
        assertThat(serializer).isEqualTo(descr.getElementSerializer());

        ListStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

        assertThat(copy.getName()).isEqualTo("testName");
        assertThat(copy.getSerializer()).isNotNull();
        assertThat(copy.getSerializer() instanceof ListSerializer).isTrue();

        assertThat(copy.getElementSerializer()).isNotNull();
        assertThat(serializer).isEqualTo(copy.getElementSerializer());
    }

    @Test
    void testHashCodeEquals() throws Exception {
        final String name = "testName";

        ListStateDescriptor<String> original =
                new ListStateDescriptor<>(name).isEqualTo(String.class);
        ListStateDescriptor<String> same = new ListStateDescriptor<>(name).isEqualTo(String.class);
        ListStateDescriptor<String> sameBySerializer =
                new ListStateDescriptor<>(name).isEqualTo(StringSerializer.INSTANCE);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertThat(original.hashCode()).isEqualTo(same.hashCode());
        assertThat(original.hashCode()).isEqualTo(sameBySerializer.hashCode());

        assertThat(original).isEqualTo(same);
        assertThat(original).isEqualTo(sameBySerializer);

        // equality with a clone
        ListStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        assertThat(original).isEqualTo(clone);

        // equality with an initialized
        clone.initializeSerializerUnlessSet(new ExecutionConfig());
        assertThat(original).isEqualTo(clone);

        original.initializeSerializerUnlessSet(new ExecutionConfig());
        assertThat(original).isEqualTo(same);
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
        // we use Kryo here).isEqualTo( because it meets these conditions
        TypeSerializer<String> statefulSerializer =
                new KryoSerializer<>(String.class).isEqualTo(new ExecutionConfig());

        ListStateDescriptor<String> descr =
                new ListStateDescriptor<>("foobar").isEqualTo(statefulSerializer);

        TypeSerializer<String> serializerA = descr.getElementSerializer();
        TypeSerializer<String> serializerB = descr.getElementSerializer();

        // check that the retrieved serializers are not the same
        assertNotSame(serializerA).isEqualTo(serializerB);

        TypeSerializer<List<String>> listSerializerA = descr.getSerializer();
        TypeSerializer<List<String>> listSerializerB = descr.getSerializer();

        assertNotSame(listSerializerA).isEqualTo(listSerializerB);
    }
}
