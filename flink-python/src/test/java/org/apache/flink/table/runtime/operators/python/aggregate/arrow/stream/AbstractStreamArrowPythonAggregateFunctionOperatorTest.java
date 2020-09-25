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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.ArrowPythonAggregateFunctionOperatorTestBase;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * Base class for Stream Arrow Python aggregate function operator tests.
 */
public abstract class AbstractStreamArrowPythonAggregateFunctionOperatorTest
	extends ArrowPythonAggregateFunctionOperatorTestBase {

	public OneInputStreamOperatorTestHarness<RowData, RowData> getTestHarness(
		Configuration config) throws Exception {
		RowType inputType = getInputType();
		RowType outputType = getOutputType();
		AbstractArrowPythonAggregateFunctionOperator operator = getTestOperator(
			config,
			new PythonFunctionInfo[]{
				new PythonFunctionInfo(
					PythonScalarFunctionOperatorTestBase.DummyPythonFunction.INSTANCE,
					new Integer[]{0})},
			inputType,
			outputType,
			new int[]{0},
			new int[]{2});

		int[] grouping = new int[] {0};
		RowDataKeySelector keySelector =
			KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(getInputType()));
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keySelector.getProducedType());
		testHarness.getStreamConfig().setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.BATCH_OP, 0.5);
		testHarness.setup(new RowDataSerializer(outputType));
		return testHarness;
	}
}
