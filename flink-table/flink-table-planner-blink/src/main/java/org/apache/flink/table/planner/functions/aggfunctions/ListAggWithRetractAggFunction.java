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
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.BinaryStringUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * built-in listagg with retraction aggregate function.
 */
public final class ListAggWithRetractAggFunction
	extends AggregateFunction<BinaryString, ListAggWithRetractAggFunction.ListAggWithRetractAccumulator> {

	private static final long serialVersionUID = -2836795091288790955L;
	private static final BinaryString lineDelimiter = BinaryString.fromString(",");

	/**
	 * The initial accumulator for listagg with retraction aggregate function.
	 */
	public static class ListAggWithRetractAccumulator {
		public ListView<BinaryString> list = new ListView<>(BinaryStringTypeInfo.INSTANCE);
		public ListView<BinaryString> retractList = new ListView<>(BinaryStringTypeInfo.INSTANCE);

		@VisibleForTesting
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ListAggWithRetractAccumulator that = (ListAggWithRetractAccumulator) o;
			return Objects.equals(list, that.list) &&
				Objects.equals(retractList, that.retractList);
		}
	}

	@Override
	public ListAggWithRetractAccumulator createAccumulator() {
		return new ListAggWithRetractAccumulator();
	}

	public void accumulate(ListAggWithRetractAccumulator acc, BinaryString value) throws Exception {
		// ignore null value
		if (value != null) {
			acc.list.add(value);
		}
	}

	public void retract(ListAggWithRetractAccumulator acc, BinaryString value) throws Exception {
		if (value != null) {
			if (!acc.list.remove(value)) {
				acc.retractList.add(value);
			}
		}
	}

	public void merge(ListAggWithRetractAccumulator acc, Iterable<ListAggWithRetractAccumulator> its) throws Exception {
		for (ListAggWithRetractAccumulator otherAcc : its) {
			// merge list of acc and other
			List<BinaryString> buffer = new ArrayList<>();
			for (BinaryString binaryString : acc.list.get()) {
				buffer.add(binaryString);
			}
			for (BinaryString binaryString : otherAcc.list.get()) {
				buffer.add(binaryString);
			}
			// merge retract list of acc and other
			List<BinaryString> retractBuffer = new ArrayList<>();
			for (BinaryString binaryString : acc.retractList.get()) {
				retractBuffer.add(binaryString);
			}
			for (BinaryString binaryString : otherAcc.retractList.get()) {
				retractBuffer.add(binaryString);
			}

			// merge list & retract list
			List<BinaryString> newRetractBuffer = new ArrayList<>();
			for (BinaryString binaryString : retractBuffer) {
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

	@Override
	public BinaryString getValue(ListAggWithRetractAccumulator acc) {
		try {
			Iterable<BinaryString> accList = acc.list.get();
			if (accList == null || !accList.iterator().hasNext()) {
				// return null when the list is empty
				return null;
			} else {
				return BinaryStringUtil.concatWs(lineDelimiter, accList);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}

	public void resetAccumulator(ListAggWithRetractAccumulator acc) {
		acc.list.clear();
		acc.retractList.clear();
	}
}
