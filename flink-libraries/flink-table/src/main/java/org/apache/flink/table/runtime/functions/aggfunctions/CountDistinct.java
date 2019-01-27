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

package org.apache.flink.table.runtime.functions.aggfunctions;

import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;

import java.util.Map;

/**
 * Built-in count distinct aggregate function.
 */
public class CountDistinct {

	/**
	 * Base function for count distinct aggregate.
	 */
	@SuppressWarnings("unchecked")
	public abstract static class CountDistinctAggFunction extends AggregateFunction<Long, GenericRow> {

		public abstract DataType getValueTypeInfo();

		@Override
		public GenericRow createAccumulator() {
			GenericRow acc = new GenericRow(3);
			// count
			acc.setLong(0, 0L);
			// map
			acc.update(1, new MapView(getValueTypeInfo(), DataTypes.LONG));
			return acc;
		}

		public void accumulate(GenericRow acc, Object input) throws Exception {
			if (input != null) {
				long count = acc.getLong(0); // never null
				MapView<Object, Long> map = (MapView<Object, Long>) acc.getField(1);
				Long valueCnt = map.get(input);
				if (valueCnt != null) {
					valueCnt += 1;
					if (valueCnt == 0) {
						map.remove(input);
					} else {
						map.put(input, valueCnt);
					}
				} else {
					map.put(input, 1L);
					// update count
					acc.update(0, count + 1);
				}
			}
		}

		public void retract(GenericRow acc, Object input) throws Exception {
			if (input != null) {
				long count = acc.getLong(0); // never null
				MapView<Object, Long> map = (MapView<Object, Long>) acc.getField(1);
				Long valueCnt = map.get(input);
				if (valueCnt != null) {
					valueCnt -= 1;
					if (valueCnt == 0) {
						map.remove(input);
						// update count
						acc.update(0, count - 1);
					} else {
						map.put(input, valueCnt);
					}
				} else {
					map.put(input, -1L);
				}
			}
		}

		public void merge(GenericRow acc, Iterable<GenericRow> it) throws Exception {
			MapView<Object, Long> map = (MapView<Object, Long>) acc.getField(1);
			long count = acc.getLong(0); // never be null
			for (GenericRow mergeAcc : it) {
				MapView<Object, Long> mergeMap = (MapView<Object, Long>) mergeAcc.getField(1);
				Iterable entries = mergeMap.entries();
				if (entries != null) {
					for (Map.Entry entry : (Iterable<Map.Entry>) entries) {
						Object key = entry.getKey();
						Long mergeCnt = (Long) entry.getValue();
						Long valueCnt = map.get(key);
						if (valueCnt != null) {
							Long mergedCnt = valueCnt + mergeCnt;
							if (mergedCnt == 0) {
								map.remove(key);
								if (valueCnt > 0) {
									count--;
								}
							} else if (mergedCnt < 0) {
								map.put(key, mergedCnt);
								if (valueCnt > 0) {
									count--;
								}
							} else {    // mergedCnt > 0
								if (valueCnt < 0) {
									count++;
								}
								map.put(key, mergedCnt);
							}
						} else {
							if (mergeCnt > 0) {
								map.put(key, mergeCnt);
								count++;
							} else if (mergeCnt < 0) {
								map.put(key, mergeCnt);
							} // ignore mergeCnt == 0
						}
					}
				}
			}
			acc.update(0, count);
		}

		public void resetAccumulator(GenericRow acc) {
			acc.setLong(0, 0L);
			MapView<Object, Long> map = (MapView<Object, Long>) acc.getField(1);
			map.clear();
		}

		@Override
		public Long getValue(GenericRow accumulator) {
			return accumulator.getLong(0);
		}

		@Override
		public DataType[] getUserDefinedInputTypes(Class[] signature) {
			if (signature.length == 1) {
				return new DataType[] {getValueTypeInfo()};
			} else if (signature.length == 0) {
				return new DataType[0];
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public DataType getAccumulatorType() {
			InternalType[] fieldTypes = new InternalType[]{
				DataTypes.LONG,
				// it will be replaced to MapViewType
				DataTypes.createGenericType(MapView.class),
			};
			String[] fieldNames = new String[]{"count", "map"};
			return new RowType(fieldTypes, fieldNames);
		}
	}

	/**
	 * Built-in byte count distinct aggregate function.
	 */
	public static class ByteCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.BYTE;
		}
	}

	/**
	 * Built-in decimal count distinct aggregate function.
	 */
	public static class DecimalCountDistinctAggFunction extends CountDistinctAggFunction {

		public final DecimalType decimalType;

		public DecimalCountDistinctAggFunction(DecimalType decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public DataType getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in double count distinct aggregate function.
	 */
	public static class DoubleCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.DOUBLE;
		}
	}

	/**
	 * Built-in float count distinct aggregate function.
	 */
	public static class FloatCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.FLOAT;
		}
	}

	/**
	 * Built-in int count distinct aggregate function.
	 */
	public static class IntCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.INT;
		}
	}

	/**
	 * Built-in long count distinct aggregate function.
	 */
	public static class LongCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.LONG;
		}
	}

	/**
	 * Built-in short count distinct aggregate function.
	 */
	public static class ShortCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.SHORT;
		}
	}

	/**
	 * Built-in boolean count distinct aggregate function.
	 */
	public static class BooleanCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.BOOLEAN;
		}
	}

	/**
	 * Built-in date count distinct aggregate function.
	 */
	public static class DateCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.DATE;
		}
	}

	/**
	 * Built-in time count distinct aggregate function.
	 */
	public static class TimeCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.TIME;
		}
	}

	/**
	 * Built-in timestamp count distinct aggregate function.
	 */
	public static class TimestampCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.TIMESTAMP;
		}
	}

	/**
	 * Built-in string count distinct aggregate function.
	 */
	public static class StringCountDistinctAggFunction extends CountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return new TypeInfoWrappedDataType(BinaryStringTypeInfo.INSTANCE);
		}
	}

}
