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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for the {@link ValueStateDescriptor}. */
public class ValueStateDescriptorTest extends TestLogger {

    @Test
    public void testHashCodeEquals() throws Exception {
        final String name = "testName";

        ValueStateDescriptor<String> original = new ValueStateDescriptor<>(name, String.class);
        ValueStateDescriptor<String> same = new ValueStateDescriptor<>(name, String.class);
        ValueStateDescriptor<String> sameBySerializer =
                new ValueStateDescriptor<>(name, StringSerializer.INSTANCE);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertEquals(original.hashCode(), same.hashCode());
        assertEquals(original.hashCode(), sameBySerializer.hashCode());

        assertEquals(original, same);
        assertEquals(original, sameBySerializer);

        // equality with a clone
        ValueStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        assertEquals(original, clone);

        // equality with an initialized
        clone.initializeSerializerUnlessSet(new ExecutionConfig());
        assertEquals(original, clone);

        original.initializeSerializerUnlessSet(new ExecutionConfig());
        assertEquals(original, same);
    }

    @Test
    public void testVeryLargeDefaultValue() throws Exception {
        // ensure that we correctly read very large data when deserializing the default value

        TypeSerializer<String> serializer =
                new KryoSerializer<>(String.class, new ExecutionConfig());
        byte[] data = new byte[200000];
        for (int i = 0; i < 200000; i++) {
            data[i] = 65;
        }
        data[199000] = '\0';

        String defaultValue = new String(data, ConfigConstants.DEFAULT_CHARSET);

        ValueStateDescriptor<String> descr =
                new ValueStateDescriptor<>("testName", serializer, defaultValue);

        assertEquals("testName", descr.getName());
        assertEquals(defaultValue, descr.getDefaultValue());
        assertNotNull(descr.getSerializer());
        assertEquals(serializer, descr.getSerializer());

        ValueStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

        assertEquals("testName", copy.getName());
        assertEquals(defaultValue, copy.getDefaultValue());
        assertNotNull(copy.getSerializer());
        assertEquals(serializer, copy.getSerializer());
    }
}
