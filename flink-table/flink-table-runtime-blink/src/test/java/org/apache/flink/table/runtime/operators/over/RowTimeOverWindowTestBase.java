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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Base class for row-time over window test.
 **/
public class RowTimeOverWindowTestBase {
	protected static GeneratedAggsHandleFunction aggsHandleFunction =
		new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
			@Override
			public AggsHandleFunction newInstance(ClassLoader classLoader) {
				return new SumAggsHandleFunction(1);
			}
		};

	protected LogicalType[] inputFieldTypes = new LogicalType[]{
		new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType(),
		new BigIntType()
	};
	protected LogicalType[] accTypes = new LogicalType[]{new BigIntType()};

	protected BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[]{0}, inputFieldTypes);
	protected TypeInformation<RowData> keyType = keySelector.getProducedType();

	protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
			KeyedProcessOperator<RowData, RowData, RowData> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}
}
