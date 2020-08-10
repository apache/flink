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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.dataview.ListViewTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.inference.TypeStrategies.explicit;

/**
 * built-in listagg with retraction aggregate function.
 */
public final class ListAggWithRetractAggFunction
	extends AggregateFunction<StringData, ListAggWithRetractAggFunction.ListAggWithRetractAccumulator> {

	private static final long serialVersionUID = -2836795091288790955L;
	private static final BinaryStringData lineDelimiter = BinaryStringData.fromString(",");

	/**
	 * The initial accumulator for listagg with retraction aggregate function.
	 */
	public static class ListAggWithRetractAccumulator {
		public ListView<StringData> list = new ListView<>(StringDataTypeInfo.INSTANCE);
		public ListView<StringData> retractList = new ListView<>(StringDataTypeInfo.INSTANCE);

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
	public TypeInformation<StringData> getResultType() {
		return StringDataTypeInfo.INSTANCE;
	}

	@Override
	public TypeInformation<ListAggWithRetractAccumulator> getAccumulatorType() {
		try {
			Class<ListAggWithRetractAccumulator> clazz = ListAggWithRetractAccumulator.class;
			List<PojoField> pojoFields = new ArrayList<>();
			pojoFields.add(new PojoField(
				clazz.getDeclaredField("list"),
				new ListViewTypeInfo<>(StringDataTypeInfo.INSTANCE)));
			pojoFields.add(new PojoField(
				clazz.getDeclaredField("retractList"),
				new ListViewTypeInfo<>(StringDataTypeInfo.INSTANCE)));
			return new PojoTypeInfo<>(clazz, pojoFields);
		} catch (NoSuchFieldException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	// updated to new type system for testing
	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInference.newBuilder()
			.typedArguments(DataTypes.STRING().bridgedTo(StringData.class))
			.accumulatorTypeStrategy(
				explicit(
					DataTypes.STRUCTURED(
						ListAggWithRetractAccumulator.class,
						DataTypes.FIELD(
							"list",
							ListView.newListViewDataType(DataTypes.STRING().bridgedTo(StringData.class))),
						DataTypes.FIELD(
							"retractList",
							ListView.newListViewDataType(DataTypes.STRING().bridgedTo(StringData.class)))
					)
				)
			)
			.outputTypeStrategy(explicit(DataTypes.STRING().bridgedTo(StringData.class)))
			.build();
	}

	@Override
	public ListAggWithRetractAccumulator createAccumulator() {
		return new ListAggWithRetractAccumulator();
	}

	public void accumulate(ListAggWithRetractAccumulator acc, StringData value) throws Exception {
		// ignore null value
		if (value != null) {
			acc.list.add(value);
		}
	}

	public void retract(ListAggWithRetractAccumulator acc, StringData value) throws Exception {
		if (value != null) {
			if (!acc.list.remove(value)) {
				acc.retractList.add(value);
			}
		}
	}

	public void merge(ListAggWithRetractAccumulator acc, Iterable<ListAggWithRetractAccumulator> its) throws Exception {
		for (ListAggWithRetractAccumulator otherAcc : its) {
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

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public StringData getValue(ListAggWithRetractAccumulator acc) {
		try {
			// we removed the element type to make the compile pass,
			// the element must be BinaryStringData because it's the only implementation.
			Iterable accList = acc.list.get();
			if (accList == null || !accList.iterator().hasNext()) {
				// return null when the list is empty
				return null;
			} else {
				return BinaryStringDataUtil.concatWs(lineDelimiter, accList);
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
