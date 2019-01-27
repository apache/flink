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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteComparator;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleComparator;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.codegen.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.codegen.GeneratedRecordComparator;
import org.apache.flink.table.codegen.SortCodeGenerator;
import org.apache.flink.table.codegen.SortCodeGeneratorTest;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.util.SortUtil;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.typeutils.TypeUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Test of sort.
 */
@RunWith(Parameterized.class)
public class InMemorySortTest {

	private final boolean order;
	private List<MemorySegment> segments;

	public InMemorySortTest(Boolean order) {
		this.order = order;
	}

	@Parameterized.Parameters
	public static List<Boolean> getOrder() {
		List<Boolean> result = new ArrayList<>();
		result.add(true);
		result.add(false);
		return result;
	}

	@Before
	public void before() {
		this.segments = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			segments.add(MemorySegmentFactory.wrap(new byte[32768]));
		}
	}

	@Test
	public void testInt() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getIntSortBase(0, order, "testInt");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.INT);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer,
				tuple2.f1, segments, 0, 0);
		List<Integer> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new IntIterator(30, 10_000);
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			data.add(row.getInt(0));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<Integer> result = new ArrayList<>();
		BinaryRow row = new BinaryRow(1);
		while ((row = iter.next(row)) != null) {
			result.add(row.getInt(0));
		}

		Collections.sort(data);
		if (!order) {
			Collections.reverse(data);
		}
		Assert.assertEquals(data, result);
	}

	@Test
	public void testLongInt() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getLongIntSortBase(order, "testLongInt");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.LONG, Types.INT);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer,
				tuple2.f1, segments, 0, 0);
		List<Tuple2<Long, Integer>> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new LongIntIterator(30, 10_000);
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			data.add(new Tuple2<>(row.getLong(0), row.getInt(1)));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<Tuple2<Long, Integer>> result = new ArrayList<>();
		BinaryRow row = new BinaryRow(2);
		while ((row = iter.next(row)) != null) {
			result.add(new Tuple2<>(row.getLong(0), row.getInt(1)));
		}

		data.sort((o1, o2) -> {
			int cmp = Long.compare(o1.f0, o2.f0);
			if (cmp == 0) {
				cmp = Integer.compare(o1.f1, o2.f1);
			}
			return cmp;
		});
		if (!order) {
			Collections.reverse(data);
		}
		Assert.assertEquals(data, result);
	}

	@Test
	public void testString() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getStringSortBase(0, order, "testString");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer, tuple2.f1, segments, 0, 0);
		List<BinaryRow> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new StringIterator(100_000_000, 10_000);
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			data.add(serializer.copy(row));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<BinaryRow> result = new ArrayList<>();
		BinaryRow row;
		while ((row = iter.next(serializer.createInstance())) != null) {
			result.add(row);
		}

		data.sort(tuple2.f1::compare);

		Assert.assertEquals(data.size(), result.size());
		for (int i = 0; i < data.size(); i++) {
			Assert.assertEquals(0, tuple2.f1.compare(data.get(i), result.get(i)));
		}
	}

	@Test
	public void testByteAndString() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getByteStringSortBase(order, "testLongInt");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.BYTE, Types.STRING);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer, tuple2.f1, segments, 0, 0);
		List<BinaryRow> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new ByteStringIterator(100_000_000, 10_000);
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			data.add(serializer.copy(row));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<BinaryRow> result = new ArrayList<>();
		BinaryRow row;
		while ((row = iter.next(serializer.createInstance())) != null) {
			result.add(row);
		}

		data.sort(tuple2.f1::compare);
		Assert.assertEquals(data.size(), result.size());
		for (int i = 0; i < data.size(); i++) {
			Assert.assertEquals(0, tuple2.f1.compare(data.get(i), result.get(i)));
		}
	}

	@Test
	public void testNullString() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getStringSortBase(0, order, "testNullString");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer, tuple2.f1, segments, 0, 0);
		List<BinaryRow> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new StringIterator(100_000_000, 10_000);
		int cnt = 0;
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			if (cnt % 100 == 0) {
				row.setNullAt(0);
			}
			data.add(serializer.copy(row));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<BinaryRow> result = new ArrayList<>();
		BinaryRow row;
		while ((row = iter.next(serializer.createInstance())) != null) {
			result.add(row);
		}

		data.sort(tuple2.f1::compare);
		Assert.assertEquals(data.size(), result.size());
		for (int i = 0; i < data.size(); i++) {
			Assert.assertEquals(0, tuple2.f1.compare(data.get(i), result.get(i)));
		}
	}

	@Test
	public void testBaseRow() throws IOException, InstantiationException, IllegalAccessException {

		TypeInformation type = new TupleTypeInfo(Types.INT, Types.INT);
		BaseRowSerializer serializer = new BaseRowSerializer(DataTypes.INT, DataTypes.INT);
		TypeComparator comparator = TypeUtils.createInternalComparator(type, order);

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getGenericSortBase(
				"testGenericTuple2", new TupleTypeInfo(Types.INT, Types.INT),
				serializer, comparator);

		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0,
				(TypeSerializer) new BinaryRowSerializer(Types.INT),
				new BinaryRowSerializer(Types.INT),
				tuple2.f1, segments, 0, 0);
		List<Tuple2<Integer, Integer>> data = new ArrayList<>();

		UniformIntTupleGenerator generator = new UniformIntTupleGenerator(300, 300, true);

		for (int i = 0; i < 10_000; i++) {
			Tuple2<Integer, Integer> next = generator.next();
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.reset();
			writer.writeBinaryRow(0, serializer.baseRowToBinary(GenericRow.of(next.f0, next.f1)));
			writer.complete();
			data.add(next);
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<Tuple2<Integer, Integer>> result = new ArrayList<>();
		BinaryRow row = new BinaryRow(1);
		while ((row = iter.next(row)) != null) {
			BaseRow baseRow = row.getBaseRow(0, 2);
			result.add(new Tuple2<>(baseRow.getInt(0), baseRow.getInt(1)));
		}

		data.sort((o1, o2) -> {
			int cmp = Integer.compare(o1.f0, o2.f0);
			if (cmp == 0) {
				cmp = Integer.compare(o1.f1, o2.f1);
			}
			return cmp;
		});
		if (!order) {
			Collections.reverse(data);
		}
		Assert.assertEquals(data, result);
	}

	@Test
	public void testStringAndDoubleSortByDouble() throws IOException, InstantiationException, IllegalAccessException {

		Tuple2<NormalizedKeyComputer, RecordComparator> tuple2 = getStringDoubleSortByDoubleBase(order, "testLongInt");

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING, Types.DOUBLE);
		BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(null,
				tuple2.f0, (TypeSerializer) serializer, serializer, tuple2.f1, segments, 0, 0);
		List<BinaryRow> data = new ArrayList<>();
		Iterator<BinaryRow> iterator = new StringDoubleIterator(100_000_000, 10_000);
		while (iterator.hasNext()) {
			BinaryRow row = iterator.next();
			data.add(serializer.copy(row));
			if (!sortBuffer.write(row)) {
				throw new RuntimeException();
			}
		}

		new QuickSort().sort(sortBuffer);

		MutableObjectIterator<BinaryRow> iter = sortBuffer.getIterator();
		List<BinaryRow> result = new ArrayList<>();
		BinaryRow row;
		while ((row = iter.next(serializer.createInstance())) != null) {
			result.add(row);
		}

		data.sort(tuple2.f1::compare);
		Assert.assertEquals(data.size(), result.size());
		for (int i = 0; i < data.size(); i++) {
			Assert.assertEquals(0, tuple2.f1.compare(data.get(i), result.get(i)));
		}
	}

	public static void writeRecordsToBuffer(
			BinaryRowSerializer serializer,
			SimpleCollectingOutputView out,
			List<Integer> expected,
			List<Long> index,
			int length) throws IOException {
		Random random = new Random();
		int stringLength = 30;
		for (int i = 0; i < length; i++) {
			index.add(out.getCurrentOffset());
			BinaryRow row = randomRow(random, stringLength);
			expected.add(row.getInt(0));
			serializer.serializeToPages(row, out);
		}
	}

	public static BinaryRow randomRow(Random random, int stringLength) {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeInt(0, random.nextInt());
		writer.writeString(1, RandomStringUtils.random(stringLength));
		writer.complete();
		return row;
	}

	/**
	 * Test pool.
	 */
	public static class TestMemorySegmentPool implements MemorySegmentPool {

		private int pageSize;

		public TestMemorySegmentPool(int pageSize) {
			this.pageSize = pageSize;
		}

		@Override
		public int pageSize() {
			return pageSize;
		}

		@Override
		public void returnAll(List<MemorySegment> memory) {
		}

		@Override
		public void clear() {
		}

		@Override
		public MemorySegment nextSegment() {
			return MemorySegmentFactory.wrap(new byte[pageSize]);
		}
	}

	/**
	 * Generate BinaryRow of a int.
	 */
	private static class IntIterator implements Iterator<BinaryRow> {
		private final int keyRange;
		private final int num;
		private final BinaryRow row = new BinaryRow(1);
		private int count = 0;
		private int rndSeed = 11;
		private Random rnd;
		private BinaryRowWriter writer = new BinaryRowWriter(row);

		IntIterator(int keyRange, int num) {
			this.keyRange = keyRange;
			this.num = num;
			this.rnd = new Random(this.rndSeed);
		}

		@Override
		public boolean hasNext() {
			return (count++ <= num);
		}

		@Override
		public BinaryRow next() {
			writer.reset();
			writer.writeInt(0, rnd.nextInt(keyRange));
			return row;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Generate BinaryRow of Long and int.
	 */
	private static class LongIntIterator implements Iterator<BinaryRow> {
		private final int keyRange;
		private final int num;
		private final BinaryRow row = new BinaryRow(2);
		private int count = 0;
		private int rndSeed = 11;
		private Random rnd;
		private BinaryRowWriter writer = new BinaryRowWriter(row);

		LongIntIterator(int keyRange, int num) {
			this.keyRange = keyRange;
			this.num = num;
			this.rnd = new Random(this.rndSeed);
		}

		@Override
		public boolean hasNext() {
			return (count++ <= num);
		}

		@Override
		public BinaryRow next() {
			writer.reset();
			writer.writeLong(0, rnd.nextInt(keyRange));
			writer.writeInt(1, rnd.nextInt(keyRange));
			return row;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Generate BinaryRow of String.
	 */
	private static class StringIterator implements Iterator<BinaryRow> {
		private final int keyRange;
		private final int num;
		private final BinaryRow row = new BinaryRow(1);
		private int count = 0;
		private int rndSeed = 11;
		private Random rnd;
		private BinaryRowWriter writer = new BinaryRowWriter(row);

		StringIterator(int keyRange, int num) {
			this.keyRange = keyRange;
			this.num = num;
			this.rnd = new Random(this.rndSeed);
		}

		@Override
		public boolean hasNext() {
			return (count++ <= num);
		}

		@Override
		public BinaryRow next() {
			writer.reset();
			writer.writeString(0, String.valueOf(rnd.nextInt(keyRange)));
			return row;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Generate BinaryRow of byte and string.
	 */
	private static class ByteStringIterator implements Iterator<BinaryRow> {
		private final int keyRange;
		private final int num;
		private final BinaryRow row = new BinaryRow(2);
		private int count = 0;
		private int rndSeed = 11;
		private Random rnd;
		private BinaryRowWriter writer = new BinaryRowWriter(row);

		ByteStringIterator(int keyRange, int num) {
			this.keyRange = keyRange;
			this.num = num;
			this.rnd = new Random(this.rndSeed);
		}

		@Override
		public boolean hasNext() {
			return (count++ <= num);
		}

		@Override
		public BinaryRow next() {
			writer.reset();
			writer.writeByte(0, (byte) rnd.nextInt(keyRange));
			writer.writeString(1, String.valueOf(rnd.nextInt(keyRange)));
			return row;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Generate BinaryRow of string and double.
	 */
	private static class StringDoubleIterator implements Iterator<BinaryRow> {
		private final int keyRange;
		private final int num;
		private final BinaryRow row = new BinaryRow(2);
		private int count = 0;
		private int rndSeed = 11;
		private Random rnd;
		private BinaryRowWriter writer = new BinaryRowWriter(row);

		StringDoubleIterator(int keyRange, int num) {
			this.keyRange = keyRange;
			this.num = num;
			this.rnd = new Random(this.rndSeed);
		}

		@Override
		public boolean hasNext() {
			return (count++ <= num);
		}

		@Override
		public BinaryRow next() {
			writer.reset();
			writer.writeString(0, String.valueOf(rnd.nextInt(keyRange)));
			writer.writeDouble(1, rnd.nextDouble());
			return row;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getIntSortBase(
			int index, boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] types = new TypeInformation[]{Types.INT};
		TypeSerializer[] serializers = new TypeSerializer[]{IntSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new IntComparator(order)};
		return getSortBase(namePrefix, types, serializers, comparators, new int[]{index}, new boolean[]{order});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getStringSortBase(
			int index, boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] types = new TypeInformation[]{Types.STRING};
		TypeSerializer[] serializers = new TypeSerializer[]{StringSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new StringComparator(order)};
		return getSortBase(namePrefix, types, serializers, comparators, new int[]{index}, new boolean[]{order});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getLongIntSortBase(
			boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] infos = new TypeInformation[]{Types.LONG, Types.INT};
		TypeSerializer[] serializers = new TypeSerializer[]{LongSerializer.INSTANCE, IntSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new LongComparator(order), new IntComparator(order)};
		return getSortBase(namePrefix, infos, serializers, comparators, new boolean[]{order, order});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getByteStringSortBase(
			boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] infos = new TypeInformation[]{Types.BYTE, Types.STRING};
		TypeSerializer[] serializers = new TypeSerializer[]{ByteSerializer.INSTANCE, StringSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new ByteComparator(order), new StringComparator(order)};
		return getSortBase(namePrefix, infos, serializers, comparators, new boolean[]{order, order});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getStringDoubleSortByDoubleBase(
			boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] infos = new TypeInformation[]{Types.DOUBLE};
		TypeSerializer[] serializers = new TypeSerializer[]{DoubleSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new DoubleComparator(order)};
		return getSortBase(namePrefix, infos, serializers, comparators, new int[]{1}, new boolean[]{order});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getGenericSortBase(
			String namePrefix, TypeInformation type, TypeSerializer serializer,
			TypeComparator comparator) throws IllegalAccessException, InstantiationException {
		TypeInformation[] infos = new TypeInformation[]{type};
		TypeSerializer[] serializers = new TypeSerializer[]{serializer};
		TypeComparator[] comparators = new TypeComparator[]{comparator};
		return getSortBase(namePrefix, infos, serializers, comparators, new boolean[]{true});
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getIntStringSortBase(
			boolean order, String namePrefix) throws IllegalAccessException, InstantiationException {
		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.STRING};
		TypeSerializer[] serializers = new TypeSerializer[]{IntSerializer.INSTANCE};
		TypeComparator[] comparators = new TypeComparator[]{new IntComparator(order)};
		return getSortBase(namePrefix, types, serializers, comparators, new int[]{0}, new boolean[]{order});
	}

	static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBase(
			String namePrefix, TypeInformation[] types, TypeSerializer[] serializers,
			TypeComparator[] comparators, boolean[] orders) throws IllegalAccessException, InstantiationException {
		int[] keys = new int[types.length];
		for (int i = 0; i < types.length; i++) {
			keys[i] = i;
		}
		return getSortBase(namePrefix, types, serializers, comparators, keys, orders);
	}

	static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBaseWithNulls(
			String namePrefix, TypeInformation[] types, TypeSerializer[] serializers,
			TypeComparator[] comparators, boolean[] orders, boolean[] nullsIsLast) throws IllegalAccessException, InstantiationException {
		int[] keys = new int[types.length];
		for (int i = 0; i < types.length; i++) {
			keys[i] = i;
		}
		return getSortBaseWithNulls(namePrefix, types, serializers, comparators, keys, orders, nullsIsLast);
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBaseWithNulls(
			String namePrefix, TypeInformation[] types, TypeSerializer[] serializers,
			TypeComparator[] comparators, int[] keys, boolean[] orders, boolean[] nullsIsLast) throws
			IllegalAccessException,
			InstantiationException {
		SortCodeGenerator generator = new SortCodeGenerator(
				keys,
				Arrays.stream(types).map(TypeConverters::createInternalTypeFromTypeInfo)
						.toArray(InternalType[]::new),
				comparators, orders, nullsIsLast);
		GeneratedNormalizedKeyComputer computer = generator.generateNormalizedKeyComputer(namePrefix + "Computer");
		GeneratedRecordComparator comparator = generator.generateRecordComparator(namePrefix + "Comparator");

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		NormalizedKeyComputer o1 = (NormalizedKeyComputer) SortCodeGeneratorTest.compile(
				cl, computer.name(), computer.code()).newInstance();
		RecordComparator o2 = (RecordComparator) SortCodeGeneratorTest.compile(
				cl, comparator.name(), comparator.code()).newInstance();
		o1.init(serializers, comparators);
		o2.init(serializers, comparators);

		return new Tuple2<>(o1, o2);
	}

	public static Tuple2<NormalizedKeyComputer, RecordComparator> getSortBase(
			String namePrefix, TypeInformation[] types, TypeSerializer[] serializers,
			TypeComparator[] comparators, int[] keys, boolean[] orders) throws
			IllegalAccessException,
			InstantiationException {
		boolean[] nullsIsLast = SortUtil.getNullDefaultOrders(orders);
		return getSortBaseWithNulls(namePrefix, types, serializers, comparators, keys, orders, nullsIsLast);
	}

}
