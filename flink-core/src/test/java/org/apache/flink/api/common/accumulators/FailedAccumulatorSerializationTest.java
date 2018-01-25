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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link FailedAccumulatorSerialization}.
 */
public class FailedAccumulatorSerializationTest extends TestLogger {

	private static final IOException TEST_EXCEPTION = new IOException("Test exception");

	/**
	 * Tests that any method call will throw the contained throwable (wrapped in an
	 * unchecked exception if it is checked).
	 */
	@Test
	public void testMethodCallThrowsException() {
		final FailedAccumulatorSerialization<Integer, Integer> accumulator = new FailedAccumulatorSerialization<>(TEST_EXCEPTION);

		try {
			accumulator.getLocalValue();
		} catch (RuntimeException re) {
			assertThat(ExceptionUtils.findThrowableWithMessage(re, TEST_EXCEPTION.getMessage()).isPresent(), is(true));
		}

		try {
			accumulator.resetLocal();
		} catch (RuntimeException re) {
			assertThat(ExceptionUtils.findThrowableWithMessage(re, TEST_EXCEPTION.getMessage()).isPresent(), is(true));
		}

		try {
			accumulator.add(1);
		} catch (RuntimeException re) {
			assertThat(ExceptionUtils.findThrowableWithMessage(re, TEST_EXCEPTION.getMessage()).isPresent(), is(true));
		}

		try {
			accumulator.merge(new IntMinimum());
		} catch (RuntimeException re) {
			assertThat(ExceptionUtils.findThrowableWithMessage(re, TEST_EXCEPTION.getMessage()).isPresent(), is(true));
		}
	}

	/**
	 * Tests that the class can be serialized and deserialized using Java serialization.
	 */
	@Test
	public void testSerialization() throws Exception {
		final FailedAccumulatorSerialization<?, ?> accumulator = new FailedAccumulatorSerialization<>(TEST_EXCEPTION);

		final byte[] serializedAccumulator = InstantiationUtil.serializeObject(accumulator);

		final FailedAccumulatorSerialization<?, ?> deserializedAccumulator = InstantiationUtil.deserializeObject(serializedAccumulator, ClassLoader.getSystemClassLoader());

		assertThat(deserializedAccumulator.getThrowable(), is(instanceOf(TEST_EXCEPTION.getClass())));
		assertThat(deserializedAccumulator.getThrowable().getMessage(), is(equalTo(TEST_EXCEPTION.getMessage())));
	}

}
