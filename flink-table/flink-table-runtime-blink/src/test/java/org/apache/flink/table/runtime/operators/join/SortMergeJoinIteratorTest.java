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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * UT for sort merge join iterators.
 */
@RunWith(Parameterized.class)
public class SortMergeJoinIteratorTest {

	private static final int MEMORY_SIZE = 40 * DEFAULT_PAGE_SIZE;
	private static final int BUFFER_MEMORY = 20;

	private final boolean leftIsSmall;
	private MemoryManager memManager;
	private IOManager ioManager;
	private BinaryRowDataSerializer serializer;

	public SortMergeJoinIteratorTest(boolean leftIsSmall) throws Exception {
		this.leftIsSmall = leftIsSmall;
	}

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Before
	public void before() throws MemoryAllocationException {
		this.memManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
		this.ioManager = new IOManagerAsync();
		this.serializer = new BinaryRowDataSerializer(1);
	}

	@Test
	public void testInner() throws Exception {
		inner(oneEmpty(), emptyList());
		inner(haveNull(), emptyList());
		inner(noJoin(), emptyList());
		inner(oneAndTwo(), newExpect1(2));
		inner(nmJoin(), newExpect1(6));
		inner(nmMultiJoin(), newExpect1(6));
	}

	@Test
	public void testOneSideOuter() throws Exception {
		List<Tuple2<BinaryRowData, BinaryRowData>> compare1;
		List<Tuple2<BinaryRowData, BinaryRowData>> compare2;
		List<Tuple2<BinaryRowData, BinaryRowData>> compare3;
		List<Tuple2<BinaryRowData, BinaryRowData>> compare4 = newExpect1(2);
		List<Tuple2<BinaryRowData, BinaryRowData>> compare5;
		List<Tuple2<BinaryRowData, BinaryRowData>> compare6;
		if (!leftIsSmall) {
			compare1 = Arrays.asList(
					newTuple(newRow(1), null),
					newTuple(newRow(2), null));
			compare2 = Collections.singletonList(newTuple(newRow(null), null));
			compare3 = Collections.singletonList(newTuple(newRow(1), null));
			compare5 = newExpect1(6);
			compare6 = newExpect1(6);
			compare6.addAll(Arrays.asList(
					newTuple(newRow(3), null),
					newTuple(newRow(5), null)
			));
		} else {
			compare1 = emptyList();
			compare2 = Arrays.asList(
					newTuple(null, newRow(null)),
					newTuple(null, newRow(null)));
			compare3 = emptyList();
			compare5 = newExpect1(6);
			compare5.add(newTuple(null, newRow(null)));
			compare6 = newExpect1(6);
			compare6.addAll(Arrays.asList(
					newTuple(null, newRow(2)),
					newTuple(null, newRow(4))
			));
		}
		oneSideOuter(oneEmpty(), compare1);
		oneSideOuter(haveNull(), compare2);
		oneSideOuter(noJoin(), compare3);
		oneSideOuter(oneAndTwo(), compare4);
		oneSideOuter(nmJoin(), compare5);
		oneSideOuter(nmMultiJoin(), compare6);
	}

	@Test
	public void testFullOuter() throws Exception {
		fullOuter(oneEmpty(), Arrays.asList(
				newTuple(newRow(1), null),
				newTuple(newRow(2), null)));
		fullOuter(haveNull(), Arrays.asList(
				newTuple(newRow(null), null),
				newTuple(null, newRow(null)),
				newTuple(null, newRow(null))));
		fullOuter(noJoin(), Collections.singletonList(newTuple(newRow(1), null)));
		fullOuter(oneAndTwo(), newExpect1(2));
		List<Tuple2<BinaryRowData, BinaryRowData>> compare5 = newExpect1(6);
		compare5.add(newTuple(null, newRow(null)));
		fullOuter(nmJoin(), compare5);
		List<Tuple2<BinaryRowData, BinaryRowData>> compare6 = newExpect1(6);
		compare6.addAll(Arrays.asList(
				newTuple(null, newRow(2)),
				newTuple(newRow(3), null),
				newTuple(null, newRow(4)),
				newTuple(newRow(5), null)));
		fullOuter(nmMultiJoin(), compare6);
	}

	private List<Tuple2<BinaryRowData, BinaryRowData>> newExpect1(int number) {
		List<Tuple2<BinaryRowData, BinaryRowData>> list = new ArrayList<>();
		for (int i = 0; i < number; i++) {
			list.add(newTuple(newRow(1), newRow(1)));
		}
		return list;
	}

	public void inner(
			Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> data,
			List<Tuple2<BinaryRowData, BinaryRowData>> compare) throws Exception {
		MutableObjectIterator input1 = data.f0;
		MutableObjectIterator input2 = data.f1;
		if (leftIsSmall) {
			input1 = data.f1;
			input2 = data.f0;
		}
		try (SortMergeInnerJoinIterator iterator = new SortMergeInnerJoinIterator(
				new BinaryRowDataSerializer(1),
				new BinaryRowDataSerializer(1),
				new MyProjection(),
				new MyProjection(),
				new IntRecordComparator(),
				input1,
				input2,
				new ResettableExternalBuffer(
						ioManager,
						new LazyMemorySegmentPool(this, memManager, BUFFER_MEMORY),
						serializer, false), new boolean[]{true})) {
			int id = 0;
			while (iterator.nextInnerJoin()) {
				RowData probe = iterator.getProbeRow();
				ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator();
				while (iter.advanceNext()) {
					RowData row = iter.getRow();
					Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
					if (leftIsSmall) {
						assertEquals(expected, new Tuple2<>(row, probe));
					} else {
						assertEquals(expected, new Tuple2<>(probe, row));
					}
				}
			}
			assertEquals(compare.size(), id);
		}
	}

	public void oneSideOuter(
			Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> data,
			List<Tuple2<BinaryRowData, BinaryRowData>> compare) throws Exception {
		MutableObjectIterator input1 = data.f0;
		MutableObjectIterator input2 = data.f1;
		if (leftIsSmall) {
			input1 = data.f1;
			input2 = data.f0;
		}
		try (SortMergeOneSideOuterJoinIterator iterator = new SortMergeOneSideOuterJoinIterator(
				new BinaryRowDataSerializer(1),
				new BinaryRowDataSerializer(1),
				new MyProjection(),
				new MyProjection(),
				new IntRecordComparator(),
				input1,
				input2,
				new ResettableExternalBuffer(
						ioManager,
						new LazyMemorySegmentPool(this, memManager, BUFFER_MEMORY),
						serializer, false), new boolean[]{true})) {
			int id = 0;
			while (iterator.nextOuterJoin()) {
				RowData probe = iterator.getProbeRow();
				if (iterator.matchKey == null) {
					Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
					if (leftIsSmall) {
						assertEquals(expected, new Tuple2<>(null, probe));
					} else {
						assertEquals(expected, new Tuple2<>(probe, null));
					}
				} else {
					ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator();
					while (iter.advanceNext()) {
						RowData row = iter.getRow();
						Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
						assertEquals(expected, new Tuple2<>(row, probe));
					}
				}
			}
			assertEquals(compare.size(), id);
		}
	}

	public void fullOuter(
			Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> data,
			List<Tuple2<BinaryRowData, BinaryRowData>> compare) throws Exception {
		MutableObjectIterator<BinaryRowData> input1 = data.f0;
		MutableObjectIterator<BinaryRowData> input2 = data.f1;
		try (SortMergeFullOuterJoinIterator iterator = new SortMergeFullOuterJoinIterator(
				new BinaryRowDataSerializer(1),
				new BinaryRowDataSerializer(1),
				new MyProjection(),
				new MyProjection(),
				new IntRecordComparator(),
				input1,
				input2,
				new ResettableExternalBuffer(
						ioManager,
						new LazyMemorySegmentPool(this, memManager, BUFFER_MEMORY),
						serializer, false),
				new ResettableExternalBuffer(
						ioManager,
						new LazyMemorySegmentPool(this, memManager, BUFFER_MEMORY),
						serializer, false), new boolean[]{true})) {
			int id = 0;
			while (iterator.nextOuterJoin()) {
				BinaryRowData matchKey = iterator.getMatchKey();
				ResettableExternalBuffer buffer1 = iterator.getBuffer1();
				ResettableExternalBuffer buffer2 = iterator.getBuffer2();

				if (matchKey == null && buffer1.size() > 0) { // left outer join.
					ResettableExternalBuffer.BufferIterator iter = buffer1.newIterator();
					while (iter.advanceNext()) {
						RowData row = iter.getRow();
						Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
						assertEquals(expected, new Tuple2<>(row, null));
					}
				} else if (matchKey == null && buffer2.size() > 0) { // right outer join.
					ResettableExternalBuffer.BufferIterator iter = buffer2.newIterator();
					while (iter.advanceNext()) {
						RowData row = iter.getRow();
						Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
						assertEquals(expected, new Tuple2<>(null, row));
					}
				} else if (matchKey != null) { // match join.
					ResettableExternalBuffer.BufferIterator iter1 = buffer1.newIterator();
					while (iter1.advanceNext()) {
						RowData row1 = iter1.getRow();
						ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator();
						while (iter2.advanceNext()) {
							RowData row2 = iter2.getRow();
							Tuple2<BinaryRowData, BinaryRowData> expected = compare.get(id++);
							assertEquals(expected, new Tuple2<>(row1, row2));
						}
					}
				} else { // bug...
					throw new RuntimeException("There is a bug.");
				}
			}
			assertEquals(compare.size(), id);
		}
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> oneEmpty() {
		return new Tuple2<>(
				new ListIterator(Arrays.asList(newRow(1), newRow(2))),
				new ListIterator(emptyList())
		);
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> oneAndTwo() {
		return new Tuple2<>(
				new ListIterator(Collections.singletonList(newRow(1))),
				new ListIterator(Arrays.asList(newRow(1), newRow(1)))
		);
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> haveNull() {
		return new Tuple2<>(
				new ListIterator(Collections.singletonList(newRow(null))),
				new ListIterator(Arrays.asList(newRow(null), newRow(null)))
		);
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> noJoin() {
		return new Tuple2<>(
				new ListIterator(Collections.singletonList(newRow(1))),
				new ListIterator(emptyList())
		);
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> nmJoin() {
		return new Tuple2<>(
				new ListIterator(Arrays.asList(newRow(1), newRow(1))),
				new ListIterator(Arrays.asList(newRow(1), newRow(1), newRow(1), newRow(null)))
		);
	}

	private Tuple2<MutableObjectIterator<BinaryRowData>, MutableObjectIterator<BinaryRowData>> nmMultiJoin() {
		return new Tuple2<>(
				new ListIterator(Arrays.asList(newRow(1), newRow(1), newRow(3), newRow(5))),
				new ListIterator(Arrays.asList(newRow(1), newRow(1), newRow(1), newRow(2), newRow(4)))
		);
	}

	public BinaryRowData newRow(Integer i) {
		BinaryRowData row = new BinaryRowData(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		if (i != null) {
			writer.writeInt(0, i);
		} else {
			writer.setNullAt(0);
		}
		writer.complete();
		return row;
	}

	public Tuple2<BinaryRowData, BinaryRowData> newTuple(BinaryRowData i, BinaryRowData j) {
		return new Tuple2<>(i, j);
	}

	/**
	 * List iterator.
	 */
	public static class ListIterator implements MutableObjectIterator<BinaryRowData> {

		private List<BinaryRowData> list;
		private int index = 0;

		public ListIterator(List<BinaryRowData> list) {
			this.list = list;
		}

		@Override
		public BinaryRowData next(BinaryRowData binaryRow) throws IOException {
			return next();
		}

		@Override
		public BinaryRowData next() throws IOException {
			if (index < list.size()) {
				return list.get(index++);
			}
			return null;
		}
	}

}
