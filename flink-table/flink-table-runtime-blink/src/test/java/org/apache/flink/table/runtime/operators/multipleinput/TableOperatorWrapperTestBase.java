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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Base class for {@link TableOperatorWrapper} related tests.
 */
public class TableOperatorWrapperTestBase {

	protected void assertInputSpecEquals(List<InputSpec> expected, List<InputSpec> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); ++i) {
			assertInputSpecEquals(expected.get(i), actual.get(i));
		}
	}

	protected void assertInputSpecEquals(InputSpec expected, InputSpec actual) {
		assertEquals(expected.getMultipleInputId(), actual.getMultipleInputId());
		assertEquals(expected.getOutputOpInputId(), actual.getOutputOpInputId());
		assertEquals(expected.getReadOrder(), actual.getReadOrder());
		assertWrapperEquals(expected.getOutput(), actual.getOutput());
	}

	protected void assertWrapperEquals(
			List<TableOperatorWrapper<?>> expected,
			List<TableOperatorWrapper<?>> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); ++i) {
			assertWrapperEquals(expected.get(i), actual.get(i));
		}
	}

	protected void assertWrapperEquals(
			TableOperatorWrapper<?> expected,
			TableOperatorWrapper<?> actual) {
		assertEquals(expected.getOperatorName(), actual.getOperatorName());
		assertEquals(expected.getAllInputTypes(), actual.getAllInputTypes());
		assertEquals(expected.getOutputType(), actual.getOutputType());
		assertEquals(expected.getManagedMemoryFraction(), actual.getManagedMemoryFraction(), 1e-6);
		assertEdgeEquals(expected.getInputEdges(), actual.getInputEdges());
		assertEdgeEquals(expected.getOutputEdges(), actual.getOutputEdges());
	}

	protected void assertEdgeEquals(
			List<TableOperatorWrapper.Edge> expected,
			List<TableOperatorWrapper.Edge> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); ++i) {
			assertEdgeEquals(expected.get(i), actual.get(i));
		}
	}

	protected void assertEdgeEquals(
			TableOperatorWrapper.Edge expected,
			TableOperatorWrapper.Edge actual) {
		assertEquals(expected.getSource().getOperatorName(), actual.getSource().getOperatorName());
		assertEquals(expected.getTarget().getOperatorName(), actual.getTarget().getOperatorName());
		assertEquals(expected.getInputId(), actual.getInputId());
	}

}
