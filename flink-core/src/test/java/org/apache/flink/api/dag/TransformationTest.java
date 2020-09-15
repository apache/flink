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

import static org.hamcrest.Matchers.contains;
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
		transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.BATCH_OP, 123);
		transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.ROCKSDB);
		assertThat(transformation.getManagedMemoryOperatorScopeUseCaseWeights().get(ManagedMemoryUseCase.BATCH_OP), is(123));
		assertThat(transformation.getManagedMemorySlotScopeUseCases(), contains(ManagedMemoryUseCase.ROCKSDB));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemoryOperatorScopeUseCaseFailWrongScope() {
		transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.PYTHON, 123);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemoryOperatorScopeUseCaseFailZeroWeight() {
		transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.BATCH_OP, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemoryOperatorScopeUseCaseFailNegativeWeight() {
		transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.BATCH_OP, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareManagedMemorySlotScopeUseCaseFailWrongScope() {
		transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.BATCH_OP);
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
