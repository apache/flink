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

package org.apache.flink.api.dag;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Transformation}.
 */
public class TransformationTest extends TestLogger {

	private Transformation<Void> transformation;

	@Before
	public void setUp() {
		transformation = new TestTransformation<>("t", null, 1);
	}

	@Test
	public void testDeclareManagedMemoryUseCase() {
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.BATCH_OP, 123);
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.ROCKSDB, 456);
		assertThat(transformation.getManagedMemoryUseCaseWeights().get(ManagedMemoryUseCase.BATCH_OP), is(123));
		assertThat(transformation.getManagedMemoryUseCaseWeights().get(ManagedMemoryUseCase.ROCKSDB), is(456));
	}

	@Test
	public void testDeclareManagedMemoryUseCaseSlotScopeIgnoreWeight() {
		// should not fail on weight <= 0
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.ROCKSDB, 0);
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.ROCKSDB, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemoryUseCaseOperatorScopeFailZeroWeight() {
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.BATCH_OP, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemoryUseCaseOperatorScopeFailNegativeWeight() {
		transformation.declareManagedMemoryUseCase(ManagedMemoryUseCase.BATCH_OP, -1);
	}

	/**
	 * A test implementation of {@link Transformation}.
	 */
	private class TestTransformation<T> extends Transformation<T> {

		public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
			super(name, outputType, parallelism);
		}

		@Override
		public Collection<Transformation<?>> getTransitivePredecessors() {
			return Collections.EMPTY_LIST;
		}
	}
}
