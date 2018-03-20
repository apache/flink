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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link AggregatingStateDescriptor}.
 */
public class AggregatingStateDescriptorTest extends TestLogger {

	/**
	 * FLINK-6775.
	 *
	 * <p>Tests that the returned serializer is duplicated. This allows to
	 * share the state descriptor.
	 */
	@Test
	public void testSerializerDuplication() {
		// we need a serializer that actually duplicates for testing (a stateful one)
		// we use Kryo here, because it meets these conditions
		TypeSerializer<Long> serializer = new KryoSerializer<>(Long.class, new ExecutionConfig());

		AggregateFunction<Long, Long, Long> aggregatingFunction = mock(AggregateFunction.class);

		AggregatingStateDescriptor<Long, Long, Long> descr = new AggregatingStateDescriptor<>(
			"foobar",
			aggregatingFunction,
			serializer);

		TypeSerializer<Long> serializerA = descr.getSerializer();
		TypeSerializer<Long> serializerB = descr.getSerializer();

		// check that the retrieved serializers are not the same
		assertNotSame(serializerA, serializerB);
	}
}
