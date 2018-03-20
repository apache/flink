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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link ReducingStateDescriptor}.
 */
public class ReducingStateDescriptorTest extends TestLogger {

	@Test
	public void testReducingStateDescriptor() throws Exception {

		ReduceFunction<String> reducer = (a, b) -> a;

		TypeSerializer<String> serializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		ReducingStateDescriptor<String> descr =
				new ReducingStateDescriptor<>("testName", reducer, serializer);

		assertEquals("testName", descr.getName());
		assertNotNull(descr.getSerializer());
		assertEquals(serializer, descr.getSerializer());
		assertEquals(reducer, descr.getReduceFunction());

		ReducingStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertNotNull(copy.getSerializer());
		assertEquals(serializer, copy.getSerializer());
	}

	@Test
	public void testHashCodeEquals() throws Exception {
		final String name = "testName";
		final ReduceFunction<String> reducer = (a, b) -> a;

		ReducingStateDescriptor<String> original = new ReducingStateDescriptor<>(name, reducer, String.class);
		ReducingStateDescriptor<String> same = new ReducingStateDescriptor<>(name, reducer, String.class);
		ReducingStateDescriptor<String> sameBySerializer = new ReducingStateDescriptor<>(name, reducer, StringSerializer.INSTANCE);

		// test that hashCode() works on state descriptors with initialized and uninitialized serializers
		assertEquals(original.hashCode(), same.hashCode());
		assertEquals(original.hashCode(), sameBySerializer.hashCode());

		assertEquals(original, same);
		assertEquals(original, sameBySerializer);

		// equality with a clone
		ReducingStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
		assertEquals(original, clone);

		// equality with an initialized
		clone.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, clone);

		original.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, same);
	}
}
