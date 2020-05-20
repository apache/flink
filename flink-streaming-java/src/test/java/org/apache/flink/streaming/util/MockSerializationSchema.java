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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * A mocked {@link SerializationSchema} that verifies that {@link SerializationSchema#open(InitializationContext)}
 * was called at most once. It also checks that the {@link InitializationContext} returns a not
 * null {@link MetricGroup}.
 *
 * <p>It does not implement any of the deserialization methods.
 */
public class MockSerializationSchema<T> implements SerializationSchema<T> {

	private boolean openCalled = false;

	@Override
	public void open(SerializationSchema.InitializationContext context) throws Exception {
		assertThat("Open was called multiple times", openCalled, is(false));
		assertThat(context.getMetricGroup(), notNullValue(MetricGroup.class));
		this.openCalled = true;
	}

	@Override
	public byte[] serialize(T element) {
		return new byte[0];
	}

	public boolean isOpenCalled() {
		return openCalled;
	}
}
