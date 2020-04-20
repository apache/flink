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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.RowDataRecordEqualiser;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Base class of tests for all kinds of GroupAgg.
 */
abstract class GroupAggFunctionTestBase {

	Time minTime = Time.milliseconds(10);

	LogicalType[] inputFieldTypes = new LogicalType[] {
			new VarCharType(VarCharType.MAX_LENGTH),
			new IntType(),
			new BigIntType() };

	InternalTypeInfo<RowData> outputType = InternalTypeInfo.ofFields(
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new BigIntType());

	LogicalType[] accTypes = new LogicalType[] { new BigIntType(), new BigIntType() };
	BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[]{0}, inputFieldTypes);
	TypeInformation<RowData> keyType = keySelector.getProducedType();
	GeneratedRecordEqualiser equaliser = new GeneratedRecordEqualiser("", "", new Object[0]) {

		private static final long serialVersionUID = 1532460173848746788L;

		@Override
		public RecordEqualiser newInstance(ClassLoader classLoader) {
			return new RowDataRecordEqualiser();
		}
	};

	GeneratedAggsHandleFunction function =
			new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
		@Override
		public AggsHandleFunction newInstance(ClassLoader classLoader) {
			return new SumAndCountAgg();
		}
	};

	RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
		outputType.toRowFieldTypes(),
		new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

	static final class SumAndCountAgg implements AggsHandleFunction {
		private long sum;
		private boolean sumIsNull;
		private long count;
		private boolean countIsNull;

		@Override
		public void open(StateDataViewStore store) throws Exception {
		}

		@Override
		public void setAccumulators(RowData acc) throws Exception {
			sumIsNull = acc.isNullAt(0);
			if (!sumIsNull) {
				sum = acc.getLong(0);
			}

			countIsNull = acc.isNullAt(1);
			if (!countIsNull) {
				count = acc.getLong(1);
			}
		}

		@Override
		public void accumulate(RowData inputRow) throws Exception {
			boolean inputIsNull = inputRow.isNullAt(1);
			if (!inputIsNull) {
				sum += inputRow.getInt(1);
				count += 1;
			}
		}

		@Override
		public void retract(RowData inputRow) throws Exception {
			boolean inputIsNull = inputRow.isNullAt(1);
			if (!inputIsNull) {
				sum -= inputRow.getInt(1);
				count -= 1;
			}
		}

		@Override
		public void merge(RowData otherAcc) throws Exception {
			boolean sumIsNullOther = otherAcc.isNullAt(0);
			if (!sumIsNullOther) {
				sum += otherAcc.getLong(0);
			}

			boolean countIsNullOther = otherAcc.isNullAt(1);
			if (!countIsNullOther) {
				count += otherAcc.getLong(1);
			}
		}

		@Override
		public void resetAccumulators() throws Exception {
			sum = 0L;
			count = 0L;
		}

		@Override
		public RowData getAccumulators() throws Exception {
			GenericRowData acc = new GenericRowData(2);
			if (!sumIsNull) {
				acc.setField(0, sum);
			}

			if (!countIsNull) {
				acc.setField(1, count);
			}

			return acc;
		}

		@Override
		public RowData createAccumulators() throws Exception {
			GenericRowData acc = new GenericRowData(2);
			acc.setField(0, 0L);
			acc.setField(1, 0L);
			return acc;
		}

		@Override
		public RowData getValue() throws Exception {
			return getAccumulators();
		}

		@Override
		public void cleanup() throws Exception {

		}

		@Override
		public void close() throws Exception {

		}
	}

}
