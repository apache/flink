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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the common/shared functionality of {@link StateDescriptor}. */
class StateDescriptorTest {

    /**
     * FLINK-6775, tests that the returned serializer is duplicated. This allows to share the state
     * descriptor across threads.
     */
    @Test
    void testSerializerDuplication() {
        // we need a serializer that actually duplicates for testing (a stateful one)
        // we use Kryo here, because it meets these conditions
        TestStateDescriptor<String> descr =
                new TestStateDescriptor<>("foobar", new GenericTypeInfo<>(String.class));

        TypeSerializer<String> serializerA = descr.getSerializer();
        TypeSerializer<String> serializerB = descr.getSerializer();

        // check that the retrieved serializers are not the same
        assertThat(serializerB).isNotSameAs(serializerA);
    }

    @Test
    void testHashCodeAndEquals() throws Exception {
        final String name = "testName";

        TestStateDescriptor<String> original =
                new TestStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);
        TestStateDescriptor<String> same =
                new TestStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);
        TestStateDescriptor<String> sameBySerializer =
                new TestStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertThat(same).hasSameHashCodeAs(original);
        assertThat(sameBySerializer).hasSameHashCodeAs(original);

        assertThat(same).isEqualTo(original);
        assertThat(sameBySerializer).isEqualTo(original);

        // equality with a clone
        TestStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        assertThat(clone).isEqualTo(original);
    }

    @Test
    void testEqualsSameNameAndTypeDifferentClass() {
        final String name = "test name";

        final TestStateDescriptor<String> descr1 =
                new TestStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);
        final OtherTestStateDescriptor<String> descr2 =
                new OtherTestStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(descr2).isNotEqualTo(descr1);
    }

    @Test
    void testStateTTlConfig() {
        TestStateDescriptor<Integer> stateDescriptor =
                new TestStateDescriptor<>("test-state", BasicTypeInfo.INT_TYPE_INFO);
        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMinutes(60)).build());
        assertThat(stateDescriptor.getTtlConfig().isEnabled()).isTrue();

        stateDescriptor.enableTimeToLive(StateTtlConfig.DISABLED);
        assertThat(stateDescriptor.getTtlConfig().isEnabled()).isFalse();
    }

    // ------------------------------------------------------------------------
    //  Mock implementations and test types
    // ------------------------------------------------------------------------

    private static class TestStateDescriptor<T> extends StateDescriptor<T> {

        private static final long serialVersionUID = 1L;

        TestStateDescriptor(String name, TypeInformation<T> typeInfo) {
            super(name, typeInfo, new SerializerConfigImpl());
        }

        @Override
        public Type getType() {
            return Type.VALUE;
        }
    }

    private static class OtherTestStateDescriptor<T> extends StateDescriptor<T> {

        private static final long serialVersionUID = 1L;

        OtherTestStateDescriptor(String name, TypeInformation<T> typeInfo) {
            super(name, typeInfo, new SerializerConfigImpl());
        }

        @Override
        public Type getType() {
            return Type.VALUE;
        }
    }
}
