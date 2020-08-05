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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Built-in LISTAGGWS with retraction aggregate function.
 */
@Internal
public final class ListAggWsWithRetractAggFunction
		extends InternalAggregateFunction<StringData, ListAggWsWithRetractAggFunction.ListAggWsWithRetractAccumulator> {

	private static final long serialVersionUID = -8627988150350160473L;

	// --------------------------------------------------------------------------------------------
	// Planning
	// --------------------------------------------------------------------------------------------

	@Override
	public DataType[] getInputDataTypes() {
		return new DataType[]{
			DataTypes.STRING().bridgedTo(StringData.class),
			DataTypes.STRING().bridgedTo(StringData.class)};
	}

	@Override
	public DataType getAccumulatorDataType() {
		return DataTypes.STRUCTURED(
			ListAggWsWithRetractAccumulator.class,
			DataTypes.FIELD(
				"list",
				ListView.newListViewDataType(DataTypes.STRING().notNull().bridgedTo(StringData.class))),
			DataTypes.FIELD(
				"retractList",
				ListView.newListViewDataType(DataTypes.STRING().notNull().bridgedTo(StringData.class))),
			DataTypes.FIELD(
				"delimiter",
				DataTypes.STRING().bridgedTo(StringData.class)));
	}

	@Override
	public DataType getOutputDataType() {
		return DataTypes.STRING().bridgedTo(StringData.class);
	}

	// --------------------------------------------------------------------------------------------
	// Runtime
	// --------------------------------------------------------------------------------------------

	/** Accumulator for LISTAGGWS with retraction. */
	public static class ListAggWsWithRetractAccumulator {
		public ListView<StringData> list;
		public ListView<StringData> retractList;
		public StringData delimiter;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ListAggWsWithRetractAccumulator that = (ListAggWsWithRetractAccumulator) o;
			return Objects.equals(list, that.list) &&
				Objects.equals(retractList, that.retractList);
		}

		@Override
		public int hashCode() {
			return Objects.hash(list, retractList);
		}
	}

	@Override
	public ListAggWsWithRetractAccumulator createAccumulator() {
		final ListAggWsWithRetractAccumulator acc = new ListAggWsWithRetractAccumulator();
		acc.list = new ListView<>();
		acc.retractList = new ListView<>();
		acc.delimiter = StringData.fromString(",");
		return acc;
	}

	public void accumulate(ListAggWsWithRetractAccumulator acc, StringData value, StringData lineDelimiter) throws Exception {
		if (value != null) {
			acc.delimiter = lineDelimiter;
			acc.list.add(value);
		}
	}

	public void retract(ListAggWsWithRetractAccumulator acc, StringData value, StringData lineDelimiter) throws Exception {
		if (value != null) {
			acc.delimiter = lineDelimiter;
			if (!acc.list.remove(value)) {
				acc.retractList.add(value);
			}
		}
	}

	public void merge(ListAggWsWithRetractAccumulator acc, Iterable<ListAggWsWithRetractAccumulator> its) throws Exception {
		for (ListAggWsWithRetractAccumulator otherAcc : its) {
			if (!otherAcc.list.get().iterator().hasNext()
				&& !otherAcc.retractList.get().iterator().hasNext()) {
				// otherAcc is empty, skip it
				continue;
			}

			acc.delimiter = otherAcc.delimiter;
			// merge list of acc and other
			List<StringData> buffer = new ArrayList<>();
			for (StringData binaryString : acc.list.get()) {
				buffer.add(binaryString);
			}
			for (StringData binaryString : otherAcc.list.get()) {
				buffer.add(binaryString);
			}
			// merge retract list of acc and other
			List<StringData> retractBuffer = new ArrayList<>();
			for (StringData binaryString : acc.retractList.get()) {
				retractBuffer.add(binaryString);
			}
			for (StringData binaryString : otherAcc.retractList.get()) {
				retractBuffer.add(binaryString);
			}

			// merge list & retract list
			List<StringData> newRetractBuffer = new ArrayList<>();
			for (StringData binaryString : retractBuffer) {
				if (!buffer.remove(binaryString)) {
					newRetractBuffer.add(binaryString);
				}
			}

			// update to acc
			acc.list.clear();
			acc.list.addAll(buffer);
			acc.retractList.clear();
			acc.retractList.addAll(newRetractBuffer);
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public StringData getValue(ListAggWsWithRetractAccumulator acc) {
		try {
			// the element must be BinaryStringData because it's the only implementation.
			Iterable<BinaryStringData> accList = (Iterable) acc.list.get();
			if (accList == null || !accList.iterator().hasNext()) {
				// return null when the list is empty
				return null;
			} else {
				return BinaryStringDataUtil.concatWs((BinaryStringData) acc.delimiter, accList);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}

	public void resetAccumulator(ListAggWsWithRetractAccumulator acc) {
		acc.delimiter = StringData.fromString(",");
		acc.list.clear();
		acc.retractList.clear();
	}
}
