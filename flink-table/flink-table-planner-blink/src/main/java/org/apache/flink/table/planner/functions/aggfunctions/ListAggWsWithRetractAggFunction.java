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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * built-in listAggWs with retraction aggregate function.
 */
public final class ListAggWsWithRetractAggFunction
	extends AggregateFunction<StringData, ListAggWsWithRetractAggFunction.ListAggWsWithRetractAccumulator> {

	private static final long serialVersionUID = -8627988150350160473L;

	/**
	 * The initial accumulator for concat with retraction aggregate function.
	 */
	public static class ListAggWsWithRetractAccumulator {
		public ListView<StringData> list = new ListView<>(StringDataTypeInfo.INSTANCE);
		public ListView<StringData> retractList = new ListView<>(StringDataTypeInfo.INSTANCE);
		public StringData delimiter = StringData.fromString(",");

		@VisibleForTesting
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
				Objects.equals(retractList, that.retractList) &&
				Objects.equals(delimiter, that.delimiter);
		}
	}

	@Override
	public ListAggWsWithRetractAccumulator createAccumulator() {
		return new ListAggWsWithRetractAccumulator();
	}

	public void accumulate(ListAggWsWithRetractAccumulator acc, StringData value, StringData lineDelimiter) throws Exception {
		// ignore null value
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
			// we removed the element type to make the compile pass,
			// the element must be BinaryStringData because it's the only implementation.
			Iterable accList = acc.list.get();
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
