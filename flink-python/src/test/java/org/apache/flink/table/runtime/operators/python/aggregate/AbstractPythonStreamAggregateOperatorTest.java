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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;

/**
 * Base class for {@link PythonStreamGroupAggregateOperatorTest} and {@link PythonStreamGroupTableAggregateOperatorTest}.
 */
public abstract class AbstractPythonStreamAggregateOperatorTest {

	private LogicalType[] getOutputLogicalType() {
		return new LogicalType[]{
			DataTypes.STRING().getLogicalType(),
			DataTypes.BIGINT().getLogicalType()
		};
	}

	protected RowType getInputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new BigIntType())));
	}

	protected RowType getOutputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new BigIntType())));
	}

	private RowType getKeyType() {
		return new RowType(Collections.singletonList(
			new RowType.RowField("f1", new VarCharType())));
	}

	int[] getGrouping() {
		return new int[]{0};
	}

	private RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(getOutputLogicalType());

	protected OneInputStreamOperatorTestHarness getTestHarness(
		Configuration config) throws Exception {
		RowType outputType = getOutputType();
		OneInputStreamOperator operator = getTestOperator(config);

		KeyedOneInputStreamOperatorTestHarness testHarness =
			new KeyedOneInputStreamOperatorTestHarness(
				operator,
				KeySelectorUtil.getRowDataSelector(getGrouping(), InternalTypeInfo.of(getInputType())),
				InternalTypeInfo.of(getKeyType()),
				1,
				1,
				0);
		testHarness.getStreamConfig().setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.PYTHON, 0.5);
		testHarness.setup(new RowDataSerializer(outputType));
		return testHarness;
	}

	protected RowData newRow(boolean accumulateMsg, Object... fields) {
		if (accumulateMsg) {
			return row(fields);
		} else {
			RowData row = row(fields);
			row.setRowKind(RowKind.DELETE);
			return row;
		}
	}

	protected void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		assertor.assertOutputEquals(message, expected, actual);
	}

	abstract OneInputStreamOperator getTestOperator(Configuration config);
}
