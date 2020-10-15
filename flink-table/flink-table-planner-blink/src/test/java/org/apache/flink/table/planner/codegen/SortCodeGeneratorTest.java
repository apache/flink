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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatTestUtil;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.operators.sort.ListMemorySegmentPool;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
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

import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.utils.RawValueDataAsserter.equivalent;
import static org.junit.Assert.assertThat;

/**
 * Random test for sort code generator.
 */
public class SortCodeGeneratorTest {

	private static final int RECORD_NUM = 3000;

	private final LogicalType[] types = new LogicalType[]{
			new BooleanType(),
			new TinyIntType(),
			new SmallIntType(),
			new IntType(),
			new BigIntType(),
			new FloatType(),
			new DoubleType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new DecimalType(18, 2),
			new DecimalType(38, 18),
			new VarBinaryType(VarBinaryType.MAX_LENGTH),
			new ArrayType(new TinyIntType()),
			RowType.of(new IntType()),
			RowType.of(RowType.of(new IntType())),
			new TypeInformationRawType<>(Types.INT),
			new TimestampType(3),
			new TimestampType(9)
	};

	private int[] fields;
	private int[] keys;
	private boolean[] orders;
	private boolean[] nullsIsLast;

	private static final DataType INT_ROW_TYPE = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())).bridgedTo(Row.class);
	private static final DataFormatConverters.DataFormatConverter INT_ROW_CONV =
			DataFormatConverters.getConverterForDataType(INT_ROW_TYPE);
	private static final TypeComparator INT_ROW_COMP = new RowTypeInfo(Types.INT).createComparator(
			new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());
	private static final DataFormatConverters.DataFormatConverter NEST_ROW_CONV =
			DataFormatConverters.getConverterForDataType(
					DataTypes.ROW(DataTypes.FIELD("f0", INT_ROW_TYPE)).bridgedTo(Row.class));
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

	private BinaryRowData row(int i, Object[][] values) {
		BinaryRowData row = new BinaryRowData(fields.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		for (int j = 0; j < fields.length; j++) {
			Object value = values[j][i];
			if (value == null) {
				writer.setNullAt(j);
			} else {
				BinaryWriter.write(
					writer,
					j,
					value,
					types[fields[j]],
					InternalSerializers.create(types[fields[j]]));
			}
		}

		writer.complete();
		return row;
	}

	private BinaryRowData[] getTestData() {
		BinaryRowData[] result = new BinaryRowData[RECORD_NUM];
		Object[][] values = new Object[fields.length][];
		for (int i = 0; i < fields.length; i++) {
			values[i] = shuffle(generateValues(types[fields[i]]));
		}
		for (int i = 0; i < RECORD_NUM; i++) {
			result[i] = row(i, values);
		}
		return result;
	}

	private Object[] generateValues(LogicalType type) {

		Random rnd = new Random();

		int seedNum = RECORD_NUM / 5;
		Object[] seeds = new Object[seedNum];
		seeds[0] = null;
		seeds[1] = value1(type, rnd);
		seeds[2] = value2(type, rnd);
		seeds[3] = value3(type, rnd);
		for (int i = 4; i < seeds.length; i++) {
			switch (type.getTypeRoot()) {
				case BOOLEAN:
					seeds[i] = rnd.nextBoolean();
					break;
				case TINYINT:
					seeds[i] = (byte) rnd.nextLong();
					break;
				case SMALLINT:
					seeds[i] = (short) rnd.nextLong();
					break;
				case INTEGER:
					seeds[i] = rnd.nextInt();
					break;
				case BIGINT:
					seeds[i] = rnd.nextLong();
					break;
				case FLOAT:
					seeds[i] = rnd.nextFloat() * rnd.nextLong();
					break;
				case DOUBLE:
					seeds[i] = rnd.nextDouble() * rnd.nextLong();
					break;
				case VARCHAR:
					seeds[i] = StringData.fromString(RandomStringUtils.random(rnd.nextInt(20)));
					break;
				case DECIMAL:
					DecimalType decimalType = (DecimalType) type;
					BigDecimal decimal = new BigDecimal(
							rnd.nextInt()).divide(
							new BigDecimal(ThreadLocalRandom.current().nextInt(1, 256)),
							ThreadLocalRandom.current().nextInt(1, 30), BigDecimal.ROUND_HALF_EVEN);
					seeds[i] = DecimalData.fromBigDecimal(decimal, decimalType.getPrecision(), decimalType.getScale());
					break;
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					TimestampType timestampType = (TimestampType) type;
					if (timestampType.getPrecision() <= 3) {
						seeds[i] = TimestampData.fromEpochMillis(rnd.nextLong());
					} else {
						seeds[i] = TimestampData.fromEpochMillis(rnd.nextLong(), rnd.nextInt(1000000));
					}
					break;
				case ARRAY:
				case VARBINARY:
					byte[] bytes = new byte[rnd.nextInt(16) + 1];
					rnd.nextBytes(bytes);
					seeds[i] = type instanceof VarBinaryType ? bytes : BinaryArrayData.fromPrimitiveArray(bytes);
					break;
				case ROW:
					RowType rowType = (RowType) type;
					if (rowType.getFields().get(0).getType().getTypeRoot() == INTEGER) {
						seeds[i] = GenericRowData.of(rnd.nextInt());
					} else {
						seeds[i] = GenericRowData.of(GenericRowData.of(rnd.nextInt()));
					}
					break;
				case RAW:
					seeds[i] = RawValueData.fromObject(rnd.nextInt());
					break;
				default:
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

	private Object value1(LogicalType type, Random rnd) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return false;
			case TINYINT:
				return Byte.MIN_VALUE;
			case SMALLINT:
				return Short.MIN_VALUE;
			case INTEGER:
				return Integer.MIN_VALUE;
			case BIGINT:
				return Long.MIN_VALUE;
			case FLOAT:
				return Float.MIN_VALUE;
			case DOUBLE:
				return Double.MIN_VALUE;
			case VARCHAR:
				return StringData.fromString("");
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return DecimalData.fromBigDecimal(new BigDecimal(Integer.MIN_VALUE),
						decimalType.getPrecision(), decimalType.getScale());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return TimestampData.fromEpochMillis(Long.MIN_VALUE);
			case ARRAY:
				byte[] bytes = new byte[rnd.nextInt(7) + 1];
				rnd.nextBytes(bytes);
				BinaryArrayData array = BinaryArrayData.fromPrimitiveArray(bytes);
				for (int i = 0; i < bytes.length; i++) {
					array.setNullByte(i);
				}
				return array;
			case VARBINARY:
				byte[] bytes2 = new byte[rnd.nextInt(7) + 1];
				rnd.nextBytes(bytes2);
				return bytes2;
			case ROW:
				return GenericRowData.of(new Object[]{null});
			case RAW:
				return RawValueData.fromObject(rnd.nextInt());
			default:
				throw new RuntimeException("Not support!");
		}
	}

	private Object value2(LogicalType type, Random rnd) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return false;
			case TINYINT:
				return (byte) 0;
			case SMALLINT:
				return (short) 0;
			case INTEGER:
				return 0;
			case BIGINT:
				return 0L;
			case FLOAT:
				return 0f;
			case DOUBLE:
				return 0d;
			case VARCHAR:
				return StringData.fromString("0");
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return DecimalData.fromBigDecimal(new BigDecimal(0),
						decimalType.getPrecision(), decimalType.getScale());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return TimestampData.fromEpochMillis(0);
			case ARRAY:
			case VARBINARY:
				byte[] bytes = new byte[rnd.nextInt(7) + 10];
				rnd.nextBytes(bytes);
				return type instanceof VarBinaryType ? bytes : BinaryArrayData.fromPrimitiveArray(bytes);
			case ROW:
				RowType rowType = (RowType) type;
				if (rowType.getFields().get(0).getType().getTypeRoot() == INTEGER) {
					return GenericRowData.of(rnd.nextInt());
				} else {
					return GenericRowData.of(GenericRowData.of(new Object[]{null}));
				}
			case RAW:
				return RawValueData.fromObject(rnd.nextInt());
			default:
				throw new RuntimeException("Not support!");
		}
	}

	private Object value3(LogicalType type, Random rnd) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return true;
			case TINYINT:
				return Byte.MAX_VALUE;
			case SMALLINT:
				return Short.MAX_VALUE;
			case INTEGER:
				return Integer.MAX_VALUE;
			case BIGINT:
				return Long.MAX_VALUE;
			case FLOAT:
				return Float.MAX_VALUE;
			case DOUBLE:
				return Double.MAX_VALUE;
			case VARCHAR:
				return StringData.fromString(RandomStringUtils.random(100));
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return DecimalData.fromBigDecimal(new BigDecimal(Integer.MAX_VALUE),
						decimalType.getPrecision(), decimalType.getScale());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return TimestampData.fromEpochMillis(Long.MAX_VALUE, 999999);
			case ARRAY:
			case VARBINARY:
				byte[] bytes = new byte[rnd.nextInt(100) + 100];
				rnd.nextBytes(bytes);
				return type instanceof VarBinaryType ? bytes : BinaryArrayData.fromPrimitiveArray(bytes);
			case ROW:
				RowType rowType = (RowType) type;
				if (rowType.getFields().get(0).getType().getTypeRoot() == INTEGER) {
					return GenericRowData.of(rnd.nextInt());
				} else {
					return GenericRowData.of(GenericRowData.of(rnd.nextInt()));
				}
			case RAW:
				return RawValueData.fromObject(rnd.nextInt());
			default:
				throw new RuntimeException("Not support!");
		}
	}

	private LogicalType[] getFieldTypes() {
		LogicalType[] result = new LogicalType[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = types[fields[i]];
		}
		return result;
	}

	private LogicalType[] getKeyTypes() {
		LogicalType[] result = new LogicalType[keys.length];
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

		LogicalType[] fieldTypes = getFieldTypes();
		LogicalType[] keyTypes = getKeyTypes();

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getSortBaseWithNulls(
				this.getClass().getSimpleName(), keyTypes, keys, orders, nullsIsLast);

		BinaryRowDataSerializer serializer = new BinaryRowDataSerializer(fieldTypes.length);

		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(
				tuple2.f0, (AbstractRowDataSerializer) serializer, serializer,
				tuple2.f1, new ListMemorySegmentPool(segments));

		BinaryRowData[] dataArray = getTestData();

		List<BinaryRowData> data = Arrays.asList(dataArray.clone());
		List<BinaryRowData> binaryRows = Arrays.asList(dataArray.clone());
		Collections.shuffle(binaryRows);

		for (BinaryRowData row : binaryRows) {
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRowData> iter = sortBuffer.getIterator();
		List<BinaryRowData> result = new ArrayList<>();
		BinaryRowData row = serializer.createInstance();
		while ((row = iter.next(row)) != null) {
			result.add(row.copy());
		}

		data.sort((o1, o2) -> {
			for (int i = 0; i < keys.length; i++) {
				LogicalType t = types[fields[keys[i]]];
				boolean order = orders[i];
				Object first = null;
				Object second = null;
				RowData.FieldGetter fieldGetter = RowData.createFieldGetter(keyTypes[i], keys[i]);
				if (!o1.isNullAt(keys[i])) {
					first = fieldGetter.getFieldOrNull(o1);
				}
				if (!o2.isNullAt(keys[i])) {
					second = fieldGetter.getFieldOrNull(o2);
				}

				if (first != null || second != null) {
					if (first == null) {
						return order ? -1 : 1;
					}
					if (second == null) {
						return order ? 1 : -1;
					}
					if (first instanceof Comparable) {
						int ret = ((Comparable) first).compareTo(second);
						if (ret != 0) {
							return order ? ret : -ret;
						}
					} else if (t.getTypeRoot() == LogicalTypeRoot.ARRAY) {
						BinaryArrayData leftArray = (BinaryArrayData) first;
						BinaryArrayData rightArray = (BinaryArrayData) second;
						int minLength = Math.min(leftArray.size(), rightArray.size());
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
						if (leftArray.size() < rightArray.size()) {
							return order ? -1 : 1;
						} else if (leftArray.size() > rightArray.size()) {
							return order ? 1 : -1;
						}
					} else if (t.getTypeRoot() == LogicalTypeRoot.VARBINARY) {
						int comp = org.apache.flink.table.runtime.operators.sort.SortUtil.compareBinary(
								(byte[]) first, (byte[]) second);
						if (comp != 0) {
							return order ? comp : -comp;
						}
					} else if (t.getTypeRoot() == LogicalTypeRoot.ROW) {
						RowType rowType = (RowType) t;
						int comp;
						if (rowType.getFields().get(0).getType() instanceof IntType) {
							comp = INT_ROW_COMP.compare(INT_ROW_CONV.toExternal(first),
									INT_ROW_CONV.toExternal(second));
						} else {
							comp = NEST_ROW_COMP.compare(NEST_ROW_CONV.toExternal(first),
									NEST_ROW_CONV.toExternal(second));
						}
						if (comp != 0) {
							return order ? comp : -comp;
						}
					} else if (t.getTypeRoot() == LogicalTypeRoot.RAW) {
						Integer i1 = ((RawValueData<Integer>) first).toObject(IntSerializer.INSTANCE);
						Integer i2 = ((RawValueData<Integer>) second).toObject(IntSerializer.INSTANCE);
						int comp = Integer.compare(i1, i2);
						if (comp != 0) {
							return order ? comp : -comp;
						}
					} else {
						throw new RuntimeException();
					}
				}
			}
			return 0;
		});

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.size(); i++) {
			builder.append("\n")
					.append("expect: ")
					.append(DataFormatTestUtil.rowDataToString(data.get(i), fieldTypes))
					.append("; actual: ")
					.append(DataFormatTestUtil.rowDataToString(result.get(i), fieldTypes));
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
					RowData.FieldGetter fieldGetter = RowData.createFieldGetter(keyTypes[j], keys[j]);
					Object o1 = fieldGetter.getFieldOrNull(data.get(i));
					Object o2 = fieldGetter.getFieldOrNull(result.get(i));
					if (keyTypes[j] instanceof VarBinaryType) {
						Assert.assertArrayEquals(msg, (byte[]) o1, (byte[]) o2);
					} else if (keyTypes[j] instanceof TypeInformationRawType) {
						assertThat(
							msg,
							(RawValueData) o1,
							equivalent((RawValueData) o2, new RawValueDataSerializer<>(IntSerializer.INSTANCE)));
					} else {
						Assert.assertEquals(msg, o1, o2);
					}
				}
			}
		}
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBaseWithNulls(
			String namePrefix, LogicalType[] keyTypes, int[] keys, boolean[] orders, boolean[] nullsIsLast)
			throws IllegalAccessException, InstantiationException {
		SortCodeGenerator generator = new SortCodeGenerator(
				new TableConfig(), keys, keyTypes, orders, nullsIsLast);
		GeneratedNormalizedKeyComputer computer = generator.generateNormalizedKeyComputer(namePrefix + "Computer");
		GeneratedRecordComparator comparator = generator.generateRecordComparator(namePrefix + "Comparator");
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		return new Tuple2<>(computer.newInstance(cl), comparator.newInstance(cl));
	}
}
