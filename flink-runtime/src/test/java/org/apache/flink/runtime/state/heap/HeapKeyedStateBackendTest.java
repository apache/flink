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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link HeapKeyedStateBackend}.
 */
public class HeapKeyedStateBackendTest extends HeapStateBackendTestBase {
	@Test
	public void testGetKeys() throws Exception {
		String fieldName = "get-keys-test";
		HeapKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
		try {
			ValueState<Integer> keyedState = backend.getOrCreateKeyedState(
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));
			((InternalValueState<VoidNamespace, Integer>) keyedState).setCurrentNamespace(VoidNamespace.INSTANCE);

			int[] expectedKeys = {0, 42, 44, 1337};
			for (int key : expectedKeys) {
				backend.setCurrentKey(key);
				keyedState.update(key * 2);
			}

			try (Stream<Integer> keysStream = backend.getKeys(fieldName, VoidNamespace.INSTANCE).sorted()) {
				int[] actualKeys = keysStream.mapToInt(value -> value.intValue()).toArray();
				assertArrayEquals(expectedKeys, actualKeys);
			}
		}
		finally {
			IOUtils.closeQuietly(backend);
			backend.dispose();
		}
	}
}
