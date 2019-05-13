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

package org.apache.flink.table.codegen;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.NormalizedKeyComputer;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.plan.util.SortUtil;
import org.apache.flink.table.runtime.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.type.ArrayType;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.GenericType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.type.RowType;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random test for sort code generator.
 */
public class SortCodeGeneratorTest {

	private static final int RECORD_NUM = 3000;

	private final InternalType[] types = new InternalType[]{
			InternalTypes.BOOLEAN,
			InternalTypes.BYTE,
			InternalTypes.SHORT,
			InternalTypes.INT,
			InternalTypes.LONG,
			InternalTypes.FLOAT,
			InternalTypes.DOUBLE,
			InternalTypes.STRING,
			new DecimalType(18, 2),
			new DecimalType(38, 18),
			InternalTypes.BINARY,
			new ArrayType(InternalTypes.BYTE),
			new RowType(InternalTypes.INT),
			new RowType(new RowType(InternalTypes.INT)),
			new GenericType<>(Types.INT)
	};

	private int[] fields;
	private int[] keys;
	private boolean[] orders;
	private boolean[] nullsIsLast;

	private static final DataFormatConverters.DataFormatConverter INT_ROW_CONV =
			DataFormatConverters.getConverterForTypeInfo(new RowTypeInfo(Types.INT));
	private static final TypeComparator INT_ROW_COMP = new RowTypeInfo(Types.INT).createComparator(
			new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());
	private static final DataFormatConverters.DataFormatConverter NEST_ROW_CONV =
			DataFormatConverters.getConverterForTypeInfo(new RowTypeInfo(new RowTypeInfo(Types.INT)));
	private static final TypeComparator NEST_ROW_COMP = new RowTypeInfo(new RowTypeInfo(Types.INT)).createComparator(
			new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

	@Test
	public void testMultiKeys() throws Exception {
		for (int i = 0; i < 100; i++) {
			randomKeysAndOrders();
			testInner();
		}
	}

	@Test
	public void testOneKey() throws Exception {
		for (int time = 0; time < 100; time++) {
			Random rnd = new Random();
			fields = new int[rnd.nextInt(9) + 1];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = rnd.nextInt(types.length);
			}

			keys = new int[] {0};
			orders = new boolean[] {rnd.nextBoolean()};
			nullsIsLast = SortUtil.getNullDefaultOrders(orders);
			testInner();
		}
	}

	private void randomKeysAndOrders() {
		Random rnd = new Random();
		fields = new int[rnd.nextInt(9) + 1];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = rnd.nextInt(types.length);
		}

		keys = new int[rnd.nextInt(fields.length) + 1];
		LinkedList<Integer> indexQueue = new LinkedList<>();
		for (int i = 0; i < fields.length; i++) {
			indexQueue.add(i);
		}
		Collections.shuffle(indexQueue);
		orders = new boolean[keys.length];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = indexQueue.poll();
			orders[i] = rnd.nextBoolean();
		}
		nullsIsLast = SortUtil.getNullDefaultOrders(orders);
	}

	private Object[] shuffle(Object[] objects) {
		Collections.shuffle(Arrays.asList(objects));
		return objects;
	}

	private BinaryRow row(int i, Object[][] values) {
		BinaryRow row = new BinaryRow(fields.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		for (int j = 0; j < fields.length; j++) {
			Object value = values[j][i];
			if (value == null) {
				writer.setNullAt(j);
			} else {
				BinaryWriter.write(writer, j, value, types[fields[j]]);
			}
		}

		writer.complete();
		return row;
	}

	private BinaryRow[] getTestData() {
		BinaryRow[] result = new BinaryRow[RECORD_NUM];
		Object[][] values = new Object[fields.length][];
		for (int i = 0; i < fields.length; i++) {
			values[i] = shuffle(generateValues(types[fields[i]]));
		}
		for (int i = 0; i < RECORD_NUM; i++) {
			result[i] = row(i, values);
		}
		return result;
	}

	private Object[] generateValues(InternalType type) {

		Random rnd = new Random();

		int seedNum = RECORD_NUM / 5;
		Object[] seeds = new Object[seedNum];
		seeds[0] = null;
		seeds[1] = value1(type, rnd);
		seeds[2] = value2(type, rnd);
		seeds[3] = value3(type, rnd);
		for (int i = 4; i < seeds.length; i++) {
			if (type.equals(InternalTypes.BOOLEAN)) {
				seeds[i] = rnd.nextBoolean();
			} else if (type.equals(InternalTypes.BYTE)) {
				seeds[i] = (byte) rnd.nextLong();
			} else if (type.equals(InternalTypes.SHORT)) {
				seeds[i] = (short) rnd.nextLong();
			} else if (type.equals(InternalTypes.INT)) {
				seeds[i] = rnd.nextInt();
			} else if (type.equals(InternalTypes.LONG)) {
				seeds[i] = rnd.nextLong();
			} else if (type.equals(InternalTypes.FLOAT)) {
				seeds[i] = rnd.nextFloat() * rnd.nextLong();
			} else if (type.equals(InternalTypes.DOUBLE)) {
				seeds[i] = rnd.nextDouble() * rnd.nextLong();
			} else if (type.equals(InternalTypes.STRING)) {
				seeds[i] = BinaryString.fromString(RandomStringUtils.random(rnd.nextInt(20)));
			} else if (type instanceof DecimalType) {
				DecimalType decimalType = (DecimalType) type;
				BigDecimal decimal = new BigDecimal(
						rnd.nextInt()).divide(
						new BigDecimal(ThreadLocalRandom.current().nextInt(1, 256)),
						ThreadLocalRandom.current().nextInt(1, 30), BigDecimal.ROUND_HALF_EVEN);
				seeds[i] = Decimal.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
			} else if (type instanceof ArrayType || type.equals(InternalTypes.BINARY)) {
				byte[] bytes = new byte[rnd.nextInt(16) + 1];
				rnd.nextBytes(bytes);
				seeds[i] = type.equals(InternalTypes.BINARY) ? bytes : BinaryArray.fromPrimitiveArray(bytes);
			} else if (type instanceof RowType) {
				RowType rowType = (RowType) type;
				if (rowType.getTypeAt(0).equals(InternalTypes.INT)) {
					seeds[i] = GenericRow.of(rnd.nextInt());
				} else {
					seeds[i] = GenericRow.of(GenericRow.of(rnd.nextInt()));
				}
			} else if (type instanceof GenericType) {
				seeds[i] = new BinaryGeneric<>(rnd.nextInt(), IntSerializer.INSTANCE);
			} else {
				throw new RuntimeException("Not support!");
			}
		}

		// result values
		Object[] results = new Object[RECORD_NUM];
		for (int i = 0; i < RECORD_NUM; i++) {
			results[i] = seeds[rnd.nextInt(seedNum)];
		}
		return results;
	}

	private Object value1(InternalType type, Random rnd) {
		if (type.equals(InternalTypes.BOOLEAN)) {
			return false;
		} else if (type.equals(InternalTypes.BYTE)) {
			return Byte.MIN_VALUE;
		} else if (type.equals(InternalTypes.SHORT)) {
			return Short.MIN_VALUE;
		} else if (type.equals(InternalTypes.INT)) {
			return Integer.MIN_VALUE;
		} else if (type.equals(InternalTypes.LONG)) {
			return Long.MIN_VALUE;
		} else if (type.equals(InternalTypes.FLOAT)) {
			return Float.MIN_VALUE;
		} else if (type.equals(InternalTypes.DOUBLE)) {
			return Double.MIN_VALUE;
		} else if (type.equals(InternalTypes.STRING)) {
			return BinaryString.fromString("");
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return Decimal.fromBigDecimal(new BigDecimal(Integer.MIN_VALUE),
					decimalType.precision(), decimalType.scale());
		} else if (type instanceof ArrayType) {
			byte[] bytes = new byte[rnd.nextInt(7) + 1];
			rnd.nextBytes(bytes);
			BinaryArray array = BinaryArray.fromPrimitiveArray(bytes);
			for (int i = 0; i < bytes.length; i++) {
				array.setNullByte(i);
			}
			return array;
		} else if (type.equals(InternalTypes.BINARY)) {
			byte[] bytes = new byte[rnd.nextInt(7) + 1];
			rnd.nextBytes(bytes);
			return bytes;
		} else if (type instanceof RowType) {
			return GenericRow.of(new Object[]{null});
		} else if (type instanceof GenericType) {
			return new BinaryGeneric<>(rnd.nextInt(), IntSerializer.INSTANCE);
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private Object value2(InternalType type, Random rnd) {
		if (type.equals(InternalTypes.BOOLEAN)) {
			return false;
		} else if (type.equals(InternalTypes.BYTE)) {
			return (byte) 0;
		} else if (type.equals(InternalTypes.SHORT)) {
			return (short) 0;
		} else if (type.equals(InternalTypes.INT)) {
			return 0;
		} else if (type.equals(InternalTypes.LONG)) {
			return 0L;
		} else if (type.equals(InternalTypes.FLOAT)) {
			return 0f;
		} else if (type.equals(InternalTypes.DOUBLE)) {
			return 0d;
		} else if (type.equals(InternalTypes.STRING)) {
			return BinaryString.fromString("0");
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return Decimal.fromBigDecimal(new BigDecimal(0),
					decimalType.precision(), decimalType.scale());
		} else if (type instanceof ArrayType || type.equals(InternalTypes.BINARY)) {
			byte[] bytes = new byte[rnd.nextInt(7) + 10];
			rnd.nextBytes(bytes);
			return type.equals(InternalTypes.BINARY) ? bytes : BinaryArray.fromPrimitiveArray(bytes);
		} else if (type instanceof RowType) {
			RowType rowType = (RowType) type;
			if (rowType.getTypeAt(0).equals(InternalTypes.INT)) {
				return GenericRow.of(rnd.nextInt());
			} else {
				return GenericRow.of(GenericRow.of(new Object[]{null}));
			}
		} else if (type instanceof GenericType) {
			return new BinaryGeneric<>(rnd.nextInt(), IntSerializer.INSTANCE);
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private Object value3(InternalType type, Random rnd) {
		if (type.equals(InternalTypes.BOOLEAN)) {
			return true;
		} else if (type.equals(InternalTypes.BYTE)) {
			return Byte.MAX_VALUE;
		} else if (type.equals(InternalTypes.SHORT)) {
			return Short.MAX_VALUE;
		} else if (type.equals(InternalTypes.INT)) {
			return Integer.MAX_VALUE;
		} else if (type.equals(InternalTypes.LONG)) {
			return Long.MAX_VALUE;
		} else if (type.equals(InternalTypes.FLOAT)) {
			return Float.MAX_VALUE;
		} else if (type.equals(InternalTypes.DOUBLE)) {
			return Double.MAX_VALUE;
		} else if (type.equals(InternalTypes.STRING)) {
			return BinaryString.fromString(RandomStringUtils.random(100));
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return Decimal.fromBigDecimal(new BigDecimal(Integer.MAX_VALUE),
					decimalType.precision(), decimalType.scale());
		} else if (type instanceof ArrayType || type.equals(InternalTypes.BINARY)) {
			byte[] bytes = new byte[rnd.nextInt(100) + 100];
			rnd.nextBytes(bytes);
			return type.equals(InternalTypes.BINARY) ? bytes : BinaryArray.fromPrimitiveArray(bytes);
		} else if (type instanceof RowType) {
			RowType rowType = (RowType) type;
			if (rowType.getTypeAt(0).equals(InternalTypes.INT)) {
				return GenericRow.of(rnd.nextInt());
			} else {
				return GenericRow.of(GenericRow.of(rnd.nextInt()));
			}
		} else if (type instanceof GenericType) {
			return new BinaryGeneric<>(rnd.nextInt(), IntSerializer.INSTANCE);
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private InternalType[] getFieldTypes() {
		InternalType[] result = new InternalType[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = types[fields[i]];
		}
		return result;
	}

	private InternalType[] getKeyTypes() {
		InternalType[] result = new InternalType[keys.length];
		for (int i = 0; i < keys.length; i++) {
			result[i] = types[fields[keys[i]]];
		}
		return result;
	}

	private void testInner() throws Exception {
		List<MemorySegment> segments = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			segments.add(MemorySegmentFactory.wrap(new byte[32768]));
		}

		InternalType[] fieldTypes = getFieldTypes();
		InternalType[] keyTypes = getKeyTypes();

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getSortBaseWithNulls(
				this.getClass().getSimpleName(), keyTypes, keys, orders, nullsIsLast);

		BinaryRowSerializer serializer = new BinaryRowSerializer(fieldTypes.length);

		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(
				tuple2.f0, (AbstractRowSerializer) serializer, serializer,
				tuple2.f1, segments);

		BinaryRow[] dataArray = getTestData();

		List<BinaryRow> data = Arrays.asList(dataArray.clone());
		List<BinaryRow> binaryRows = Arrays.asList(dataArray.clone());
		Collections.shuffle(binaryRows);

		for (BinaryRow row : binaryRows) {
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<BinaryRow> result = new ArrayList<>();
		BinaryRow row = serializer.createInstance();
		while ((row = iter.next(row)) != null) {
			result.add(row.copy());
		}

		data.sort((o1, o2) -> {
			for (int i = 0; i < keys.length; i++) {
				InternalType t = types[fields[keys[i]]];
				boolean order = orders[i];
				Object first = null;
				Object second = null;
				if (!o1.isNullAt(keys[i])) {
					first = TypeGetterSetters.get(o1, keys[i], keyTypes[i]);
				}
				if (!o2.isNullAt(keys[i])) {
					second = TypeGetterSetters.get(o2, keys[i], keyTypes[i]);
				}

				if (first == null && second == null) {
				} else if (first == null) {
					return order ? -1 : 1;
				} else if (second == null) {
					return order ? 1 : -1;
				} else if (first instanceof Comparable) {
					int ret = ((Comparable) first).compareTo(second);
					if (ret != 0) {
						return order ? ret : -ret;
					}
				} else if (t instanceof ArrayType) {
					BinaryArray leftArray = (BinaryArray) first;
					BinaryArray rightArray = (BinaryArray) second;
					int minLength = Math.min(leftArray.numElements(), rightArray.numElements());
					for (int j = 0; j < minLength; j++) {
						boolean isNullLeft = leftArray.isNullAt(j);
						boolean isNullRight = rightArray.isNullAt(j);
						if (isNullLeft && isNullRight) {
							// Do nothing.
						} else if (isNullLeft) {
							return order ? -1 : 1;
						} else if (isNullRight) {
							return order ? 1 : -1;
						} else {
							int comp = Byte.compare(leftArray.getByte(j), rightArray.getByte(j));
							if (comp != 0) {
								return order ? comp : -comp;
							}
						}
					}
					if (leftArray.numElements() < rightArray.numElements()) {
						return order ? -1 : 1;
					} else if (leftArray.numElements() > rightArray.numElements()) {
						return order ? 1 : -1;
					}
				} else if (t.equals(InternalTypes.BINARY)) {
					int comp = org.apache.flink.table.runtime.sort.SortUtil.compareBinary(
							(byte[]) first, (byte[]) second);
					if (comp != 0) {
						return order ? comp : -comp;
					}
				} else if (t instanceof RowType) {
					RowType rowType = (RowType) t;
					int comp;
					if (rowType.getTypeAt(0).equals(InternalTypes.INT)) {
						comp = INT_ROW_COMP.compare(INT_ROW_CONV.toExternal(first),
								INT_ROW_CONV.toExternal(second));
					} else {
						comp = NEST_ROW_COMP.compare(NEST_ROW_CONV.toExternal(first),
								NEST_ROW_CONV.toExternal(second));
					}
					if (comp != 0) {
						return order ? comp : -comp;
					}
				} else if (t instanceof GenericType) {
					Integer i1 = BinaryGeneric.getJavaObjectFromBinaryGeneric((BinaryGeneric) first, IntSerializer.INSTANCE);
					Integer i2 = BinaryGeneric.getJavaObjectFromBinaryGeneric((BinaryGeneric) second, IntSerializer.INSTANCE);
					int comp = Integer.compare(i1, i2);
					if (comp != 0) {
						return order ? comp : -comp;
					}
				} else {
					throw new RuntimeException();
				}
			}
			return 0;
		});

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.size(); i++) {
			builder.append("\n")
					.append("expect: ")
					.append(data.get(i).toOriginString(fieldTypes))
					.append("; actual: ")
					.append(result.get(i).toOriginString(fieldTypes));
		}
		builder.append("\n").append("types: ").append(Arrays.asList(fieldTypes));
		builder.append("\n").append("keys: ").append(Arrays.toString(keys));
		String msg = builder.toString();
		for (int i = 0; i < data.size(); i++) {
			for (int j = 0; j < keys.length; j++) {
				boolean isNull1 = data.get(i).isNullAt(keys[j]);
				boolean isNull2 = result.get(i).isNullAt(keys[j]);
				Assert.assertEquals(msg, isNull1, isNull2);
				if (!isNull1 || !isNull2) {
					Object o1 = TypeGetterSetters.get(data.get(i), keys[j], keyTypes[j]);
					Object o2 = TypeGetterSetters.get(result.get(i), keys[j], keyTypes[j]);
					if (keyTypes[j].equals(InternalTypes.BINARY)) {
						Assert.assertArrayEquals(msg, (byte[]) o1, (byte[]) o2);
					} else {
						Assert.assertEquals(msg, o1, o2);
					}
				}
			}
		}
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBaseWithNulls(
			String namePrefix, InternalType[] keyTypes, int[] keys, boolean[] orders, boolean[] nullsIsLast)
			throws IllegalAccessException, InstantiationException {
		SortCodeGenerator generator = new SortCodeGenerator(new TableConfig(), keys, keyTypes, orders, nullsIsLast);
		GeneratedNormalizedKeyComputer computer = generator.generateNormalizedKeyComputer(namePrefix + "Computer");
		GeneratedRecordComparator comparator = generator.generateRecordComparator(namePrefix + "Comparator");
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		return new Tuple2<>(computer.newInstance(cl), comparator.newInstance(cl));
	}
}
