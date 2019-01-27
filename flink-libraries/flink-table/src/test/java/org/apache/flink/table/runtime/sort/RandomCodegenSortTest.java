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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.NullAwareComparator;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.plan.util.SortUtil;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.typeutils.TypeUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import scala.Function1;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.CHAR_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.toOriginString;
import static org.apache.flink.table.runtime.conversion.DataStructureConverters.createToInternalConverter;

/**
 * Random codegen sort test.
 */
public class RandomCodegenSortTest {

	private static final int RECORD_NUM = 5000;

	private TupleTypeInfo tupleInt = new TupleTypeInfo<>(INT_TYPE_INFO);
	private TupleTypeInfo tupleIntString = new TupleTypeInfo<>(INT_TYPE_INFO, STRING_TYPE_INFO);
	private TupleTypeInfo tupleDouble = new TupleTypeInfo<>(DOUBLE_TYPE_INFO);
	protected final TypeInformation[] types = new TypeInformation[]{
			BOOLEAN_TYPE_INFO,
			BYTE_TYPE_INFO,
			SHORT_TYPE_INFO,
			INT_TYPE_INFO,
			LONG_TYPE_INFO,
			FLOAT_TYPE_INFO,
			DOUBLE_TYPE_INFO,
			CHAR_TYPE_INFO,
			STRING_TYPE_INFO,
			tupleInt,
			tupleIntString,
			tupleDouble,
			BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
			new BigDecimalTypeInfo(18, 2),
			BIG_DEC_TYPE_INFO
	};
	private final InternalType[] internalTypes = Arrays.stream(types)
			.map(TypeConverters::createInternalTypeFromTypeInfo)
			.toArray(InternalType[]::new);
	private final TypeSerializer[] serializers = new TypeSerializer[types.length];
	{
		for (int i = 0; i < types.length; i++) {
			serializers[i] = DataTypes.createInternalSerializer(internalTypes[i]);
		}
	}

	protected int[] fields;
	protected int[] keys;
	protected boolean[] orders;
	protected boolean[] nullsIsLast;

	@Test
	public void test() throws Exception {
		for (int i = 0; i < 100; i++) {
			randomKeysAndOrders();
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
				BaseRowUtil.write(writer, j, value, internalTypes[fields[j]], serializers[fields[j]]);
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

	private Object[] generateValues(TypeInformation type) {

		Random rnd = new Random();

		int seedNum = RECORD_NUM / 5;
		Object[] seeds = new Object[seedNum];
		seeds[0] = null;
		seeds[1] = value1(type, rnd);
		seeds[2] = value2(type, rnd);
		seeds[3] = value3(type, rnd);
		Function1<Object, Object> converter = createToInternalConverter(new TypeInfoWrappedDataType(type));
		for (int i = 4; i < seeds.length; i++) {
			if (type.equals(BOOLEAN_TYPE_INFO)) {
				seeds[i] = rnd.nextBoolean();
			} else if (type.equals(BYTE_TYPE_INFO)) {
				seeds[i] = (byte) rnd.nextLong();
			} else if (type.equals(SHORT_TYPE_INFO)) {
				seeds[i] = (short) rnd.nextLong();
			} else if (type.equals(INT_TYPE_INFO)) {
				seeds[i] = rnd.nextInt();
			} else if (type.equals(LONG_TYPE_INFO)) {
				seeds[i] = rnd.nextLong();
			} else if (type.equals(FLOAT_TYPE_INFO)) {
				seeds[i] = rnd.nextFloat() * rnd.nextLong();
			} else if (type.equals(DOUBLE_TYPE_INFO)) {
				seeds[i] = rnd.nextDouble() * rnd.nextLong();
			} else if (type.equals(CHAR_TYPE_INFO)) {
				seeds[i] = (char) rnd.nextInt();
			} else if (type.equals(STRING_TYPE_INFO)) {
				seeds[i] = converter.apply(RandomStringUtils.random(rnd.nextInt(20)));
			} else if (type.equals(tupleInt)) {
				seeds[i] = converter.apply(new Tuple1<>(rnd.nextInt()));
			} else if (type.equals(tupleIntString)) {
				seeds[i] = converter.apply(new Tuple2<>(rnd.nextInt(),
						RandomStringUtils.random(rnd.nextInt(20))));
			} else if (type.equals(tupleDouble)) {
				seeds[i] = converter.apply(new Tuple1<>(rnd.nextDouble() * rnd.nextLong()));
			} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
				byte[] bytes = new byte[rnd.nextInt(30) + 1];
				rnd.nextBytes(bytes);
				seeds[i] = bytes;
			} else if (type.equals(BIG_DEC_TYPE_INFO)) {
				BigDecimal decimal = new BigDecimal(
						randomBigInteger()).divide(
								new BigDecimal(ThreadLocalRandom.current().nextInt(1, 256)),
						ThreadLocalRandom.current().nextInt(1, 30), BigDecimal.ROUND_HALF_EVEN);
				seeds[i] = converter.apply(decimal);
			} else if (type instanceof BigDecimalTypeInfo) {
				BigDecimal decimal = new BigDecimal(
						rnd.nextInt()).divide(
						new BigDecimal(ThreadLocalRandom.current().nextInt(1, 256)),
						ThreadLocalRandom.current().nextInt(1, 30), BigDecimal.ROUND_HALF_EVEN);
				seeds[i] = converter.apply(decimal);
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

	private BigInteger randomBigInteger(){
		return new BigInteger(64, ThreadLocalRandom.current());
	}

	private Object value1(TypeInformation type, Random rnd) {
		Function1<Object, Object> converter = createToInternalConverter(new TypeInfoWrappedDataType(type));
		if (type.equals(BOOLEAN_TYPE_INFO)) {
			return false;
		} else if (type.equals(BYTE_TYPE_INFO)) {
			return Byte.MIN_VALUE;
		} else if (type.equals(SHORT_TYPE_INFO)) {
			return Short.MIN_VALUE;
		} else if (type.equals(INT_TYPE_INFO)) {
			return Integer.MIN_VALUE;
		} else if (type.equals(LONG_TYPE_INFO)) {
			return Long.MIN_VALUE;
		} else if (type.equals(FLOAT_TYPE_INFO)) {
			return Float.MIN_VALUE;
		} else if (type.equals(DOUBLE_TYPE_INFO)) {
			return Double.MIN_VALUE;
		} else if (type.equals(CHAR_TYPE_INFO)) {
			return '1';
		} else if (type.equals(STRING_TYPE_INFO)) {
			return BinaryString.fromString("");
		} else if (type.equals(tupleInt)) {
			return converter.apply(new Tuple1<>(Integer.MIN_VALUE));
		} else if (type.equals(tupleIntString)) {
			return converter.apply(new Tuple2<>(Integer.MIN_VALUE, ""));
		} else if (type.equals(tupleDouble)) {
			return converter.apply(new Tuple1<>(Double.MIN_VALUE));
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			byte[] bytes = new byte[rnd.nextInt(7) + 1];
			rnd.nextBytes(bytes);
			return bytes;
		} else if (type.equals(BIG_DEC_TYPE_INFO)) {
			return converter.apply(new BigDecimal(Long.MIN_VALUE));
		} else if (type instanceof BigDecimalTypeInfo) {
			return converter.apply(new BigDecimal(Integer.MIN_VALUE));
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private Object value2(TypeInformation type, Random rnd) {
		Function1<Object, Object> converter = createToInternalConverter(new TypeInfoWrappedDataType(type));
		if (type.equals(BOOLEAN_TYPE_INFO)) {
			return false;
		} else if (type.equals(BYTE_TYPE_INFO)) {
			return (byte) 0;
		} else if (type.equals(SHORT_TYPE_INFO)) {
			return (short) 0;
		} else if (type.equals(INT_TYPE_INFO)) {
			return 0;
		} else if (type.equals(LONG_TYPE_INFO)) {
			return 0L;
		} else if (type.equals(FLOAT_TYPE_INFO)) {
			return 0f;
		} else if (type.equals(DOUBLE_TYPE_INFO)) {
			return 0d;
		} else if (type.equals(CHAR_TYPE_INFO)) {
			return '0';
		} else if (type.equals(STRING_TYPE_INFO)) {
			return converter.apply("0");
		} else if (type.equals(tupleInt)) {
			return converter.apply(new Tuple1<>(0));
		} else if (type.equals(tupleIntString)) {
			return converter.apply(new Tuple2<>(0, "0"));
		} else if (type.equals(tupleDouble)) {
			return converter.apply(new Tuple1<>(0d));
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			byte[] bytes = new byte[rnd.nextInt(7) + 10];
			rnd.nextBytes(bytes);
			return bytes;
		} else if (type.equals(BIG_DEC_TYPE_INFO)) {
			return converter.apply(new BigDecimal(0));
		} else if (type instanceof BigDecimalTypeInfo) {
			return converter.apply(new BigDecimal(0));
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private Object value3(TypeInformation type, Random rnd) {
		Function1<Object, Object> converter = createToInternalConverter(new TypeInfoWrappedDataType(type));
		if (type.equals(BOOLEAN_TYPE_INFO)) {
			return true;
		} else if (type.equals(BYTE_TYPE_INFO)) {
			return Byte.MAX_VALUE;
		} else if (type.equals(SHORT_TYPE_INFO)) {
			return Short.MAX_VALUE;
		} else if (type.equals(INT_TYPE_INFO)) {
			return Integer.MAX_VALUE;
		} else if (type.equals(LONG_TYPE_INFO)) {
			return Long.MAX_VALUE;
		} else if (type.equals(FLOAT_TYPE_INFO)) {
			return Float.MAX_VALUE;
		} else if (type.equals(DOUBLE_TYPE_INFO)) {
			return Double.MAX_VALUE;
		} else if (type.equals(CHAR_TYPE_INFO)) {
			return '鼎';
		} else if (type.equals(STRING_TYPE_INFO)) {
			return converter.apply(RandomStringUtils.random(100));
		} else if (type.equals(tupleInt)) {
			return converter.apply(new Tuple1<>(Integer.MAX_VALUE));
		} else if (type.equals(tupleIntString)) {
			return converter.apply(new Tuple2<>(Integer.MAX_VALUE, RandomStringUtils.random(100)));
		} else if (type.equals(tupleDouble)) {
			return converter.apply(new Tuple1<>(Double.MAX_VALUE));
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			byte[] bytes = new byte[rnd.nextInt(100) + 100];
			rnd.nextBytes(bytes);
			return bytes;
		} else if (type.equals(BIG_DEC_TYPE_INFO)) {
			return converter.apply(new BigDecimal(Long.MAX_VALUE));
		} else if (type instanceof BigDecimalTypeInfo) {
			return converter.apply(new BigDecimal(Integer.MAX_VALUE));
		} else {
			throw new RuntimeException("Not support!");
		}
	}

	private TypeInformation[] getFieldTypes() {
		TypeInformation[] result = new TypeInformation[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = types[fields[i]];
		}
		return result;
	}

	private TypeInformation[] getKeyTypes() {
		TypeInformation[] result = new TypeInformation[keys.length];
		for (int i = 0; i < keys.length; i++) {
			result[i] = types[fields[keys[i]]];
		}
		return result;
	}

	private TypeSerializer[] getFieldSerializers() {
		TypeSerializer[] result = new TypeSerializer[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = serializers[fields[i]];
		}
		return result;
	}

	private TypeSerializer[] getKeySerializers() {
		TypeSerializer[] result = new TypeSerializer[keys.length];
		for (int i = 0; i < keys.length; i++) {
			result[i] = serializers[fields[keys[i]]];
		}
		return result;
	}

	private TypeComparator[] getComparators() {
		TypeComparator[] result = new TypeComparator[keys.length];
		for (int i = 0; i < keys.length; i++) {
			result[i] = TypeUtils.createInternalComparator(types[fields[keys[i]]], orders[i]);
		}
		return result;
	}

	protected void testInner() throws Exception {
		List<MemorySegment> segments = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			segments.add(MemorySegmentFactory.wrap(new byte[32768]));
		}

		TypeInformation[] fieldTypes = getFieldTypes();
		TypeInformation[] keyTypes = getKeyTypes();
		TypeSerializer[] fieldSerializers = getFieldSerializers();
		TypeSerializer[] keySerializers = getKeySerializers();
		TypeComparator[] comparators = getComparators();
		TypeComparator[] nullAwareComparators = new TypeComparator<?>[comparators.length];
		for (int i = 0; i < comparators.length; i++) {
			nullAwareComparators[i] = new NullAwareComparator<>(comparators[i], orders[i]);
		}

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = InMemorySortTest.getSortBaseWithNulls(
				this.getClass().getSimpleName(), keyTypes, keySerializers, comparators, keys, orders, nullsIsLast);

		BinaryRowSerializer serializer = new BinaryRowSerializer(fieldTypes);

		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer,
				tuple2.f1, segments, 0, 0);

		BinaryRow[] dataArray = getTestData();

		List<BinaryRow> data = Arrays.asList(dataArray);
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
				Object v1 = null;
				Object v2 = null;
				if (!o1.isNullAt(keys[i])) {
					if (keyTypes[i].equals(STRING_TYPE_INFO)) {
						v1 = o1.getBinaryString(keys[i]);
					} else {
						v1 = BaseRowUtil.get(o1, keys[i], keyTypes[i], keySerializers[i]);
					}
				}
				if (!o2.isNullAt(keys[i])) {
					if (keyTypes[i].equals(STRING_TYPE_INFO)) {
						v2 = o2.getBinaryString(keys[i]);
					} else {
						v2 = BaseRowUtil.get(o2, keys[i], keyTypes[i], keySerializers[i]);
					}
				}
				int cmp;
				if (keyTypes[i] == BYTE_PRIMITIVE_ARRAY_TYPE_INFO) {
					cmp = compareNullArray((byte[]) v1, (byte[]) v2, orders[i]);
				} else {
					cmp = nullAwareComparators[i].compare(v1, v2);
				}
				if (cmp != 0) {
					return cmp;
				}
			}
			return 0;
		});

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.size(); i++) {
			builder.append("\n")
					.append("expect: ")
					.append(toOriginString(data.get(i), fieldTypes, fieldSerializers))
					.append("; actual: ")
					.append(toOriginString(result.get(i), fieldTypes, fieldSerializers))
			;
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
					Assert.assertTrue(msg, comparators[j].compare(
							BaseRowUtil.get(data.get(i), keys[j], keyTypes[j], keySerializers[j]),
							BaseRowUtil.get(result.get(i), keys[j], keyTypes[j], keySerializers[j])) == 0);
				}
			}
		}
	}

	private static int compareNullArray(byte[] first, byte[] second, boolean order) {
		int cmp;
		if (first == null && second == null) {
			return 0;
		} else if (first == null) {
			cmp = -1;
		} else if (second == null) {
			cmp = 1;
		} else {
			cmp = BinaryRowUtil.compareByteArray(first, second);
		}
		return order ? cmp : -cmp;
	}
}
