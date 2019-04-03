/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedJoinCondition;
import org.apache.flink.table.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.JoinCondition;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation that realizes the joining through a sort-merge join strategy.
 * 1.In most cases, its performance is weaker than HashJoin.
 * 2.It is more stable than HashJoin, and most of the data can be sorted stably.
 * 3.SortMergeJoin should be the best choice if sort can be omitted in the case of multi-level join
 * cascade with the same key.
 *
 * <p>NOTE: SEMI and ANTI join output input1 instead of input2. (Contrary to {@link HashJoinOperator}).
 */
public class SortMergeJoinOperator extends TableStreamOperator<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long reservedSortMemory1;
	private final long reservedSortMemory2;
	private final long externalBufferMemory;
	private final SortMergeJoinType type;
	private final boolean leftIsSmaller;
	private final boolean[] filterNulls;

	// generated code to cook
	private GeneratedJoinCondition condFuncCode;
	private GeneratedProjection projectionCode1;
	private GeneratedProjection projectionCode2;
	private GeneratedNormalizedKeyComputer computer1;
	private GeneratedRecordComparator comparator1;
	private GeneratedNormalizedKeyComputer computer2;
	private GeneratedRecordComparator comparator2;
	private GeneratedRecordComparator genKeyComparator;

	private transient MemoryManager memManager;
	private transient IOManager ioManager;
	private transient BinaryRowSerializer serializer1;
	private transient BinaryRowSerializer serializer2;
	private transient BinaryExternalSorter sorter1;
	private transient BinaryExternalSorter sorter2;
	private transient SortMergeJoinIterator joinIterator1;
	private transient SortMergeJoinIterator joinIterator2;
	private transient SortMergeFullOuterJoinIterator fullOuterJoinIterator;
	private transient Collector<BaseRow> collector;
	private transient boolean[] isFinished;
	private transient JoinCondition condFunc;
	private transient RecordComparator keyComparator;
	private transient Projection<BaseRow, BinaryRow> projection1;
	private transient Projection<BaseRow, BinaryRow> projection2;

	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;

	private transient SortMergeJoinHelper helper;

	@VisibleForTesting
	public SortMergeJoinOperator(
			long reservedSortMemory, long externalBufferMemory, SortMergeJoinType type, boolean leftIsSmaller,
			GeneratedJoinCondition condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedNormalizedKeyComputer computer1, GeneratedRecordComparator comparator1,
			GeneratedNormalizedKeyComputer computer2, GeneratedRecordComparator comparator2,
			GeneratedRecordComparator genKeyComparator,
			boolean[] filterNulls) {
		this(reservedSortMemory, reservedSortMemory,
				externalBufferMemory, type, leftIsSmaller, condFuncCode, projectionCode1, projectionCode2, computer1,
				comparator1, computer2, comparator2, genKeyComparator, filterNulls);
	}

	public SortMergeJoinOperator(
			long reservedSortMemory1,
			long reservedSortMemory2,
			long externalBufferMemory, SortMergeJoinType type, boolean leftIsSmaller,
			GeneratedJoinCondition condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedNormalizedKeyComputer computer1, GeneratedRecordComparator comparator1,
			GeneratedNormalizedKeyComputer computer2, GeneratedRecordComparator comparator2,
			GeneratedRecordComparator genKeyComparator,
			boolean[] filterNulls) {
		this.reservedSortMemory1 = reservedSortMemory1;
		this.reservedSortMemory2 = reservedSortMemory2;
		this.externalBufferMemory = externalBufferMemory;
		this.type = type;
		this.leftIsSmaller = leftIsSmaller;
		this.condFuncCode = condFuncCode;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.computer1 = checkNotNull(computer1);
		this.comparator1 = checkNotNull(comparator1);
		this.computer2 = checkNotNull(computer2);
		this.comparator2 = checkNotNull(comparator2);
		this.genKeyComparator = checkNotNull(genKeyComparator);
		this.filterNulls = filterNulls;
	}

	@Override
	public void open() throws Exception {
		super.open();

		Configuration conf = getContainingTask().getJobConfiguration();

		isFinished = new boolean[2];
		isFinished[0] = false;
		isFinished[1] = false;

		collector = new StreamRecordCollector<>(output);

		ClassLoader cl = getUserCodeClassloader();
		AbstractRowSerializer inputSerializer1 = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn1(cl);
		this.serializer1 = new BinaryRowSerializer(inputSerializer1.getArity());

		AbstractRowSerializer inputSerializer2 = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn2(cl);
		this.serializer2 = new BinaryRowSerializer(inputSerializer2.getArity());

		this.memManager = this.getContainingTask().getEnvironment().getMemoryManager();
		this.ioManager = this.getContainingTask().getEnvironment().getIOManager();

		// sorter1
		this.sorter1 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory1,
				ioManager, inputSerializer1, serializer1,
				computer1.newInstance(cl), comparator1.newInstance(cl), conf);
		this.sorter1.startThreads();

		// sorter2
		this.sorter2 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory2, ioManager, inputSerializer2, serializer2,
				computer2.newInstance(cl), comparator2.newInstance(cl), conf);
		this.sorter2.startThreads();

		keyComparator = genKeyComparator.newInstance(cl);
		this.condFunc = condFuncCode.newInstance(cl);

		projection1 = projectionCode1.newInstance(cl);
		projection2 = projectionCode2.newInstance(cl);

		this.leftNullRow = new GenericRow(serializer1.getArity());
		this.rightNullRow = new GenericRow(serializer2.getArity());
		JoinedRow joinedRow = new JoinedRow();

		this.helper = new SortMergeJoinHelper(collector, condFunc, leftNullRow, rightNullRow, joinedRow);

		condFuncCode = null;
		computer1 = null;
		comparator1 = null;
		computer2 = null;
		comparator2 = null;
		projectionCode1 = null;
		projectionCode2 = null;
		genKeyComparator = null;

		getMetricGroup().gauge("memoryUsedSizeInBytes",
			(Gauge<Long>) () -> sorter1.getUsedMemoryInBytes() + sorter2.getUsedMemoryInBytes());

		getMetricGroup().gauge("numSpillFiles",
			(Gauge<Long>) () -> sorter1.getNumSpillFiles() + sorter2.getNumSpillFiles());

		getMetricGroup().gauge("spillInBytes",
			(Gauge<Long>) () -> sorter1.getSpillInBytes() + sorter2.getSpillInBytes());
	}

	@Override
	public void processElement1(StreamRecord<BaseRow> element) throws Exception {
		this.sorter1.write(element.getValue());
	}

	@Override
	public void processElement2(StreamRecord<BaseRow> element) throws Exception {
		this.sorter2.write(element.getValue());
	}

	public void endInput1() throws Exception {
		isFinished[0] = true;
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	public void endInput2() throws Exception {
		isFinished[1] = true;
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	private void doSortMergeJoin() throws Exception {
		MutableObjectIterator iterator1 = sorter1.getIterator();
		MutableObjectIterator iterator2 = sorter2.getIterator();

		if (type.equals(SortMergeJoinType.INNER)) {
			if (!leftIsSmaller) {
				joinIterator2 = new SortMergeInnerJoinIterator(
						serializer1, serializer2, projection1, projection2,
						keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
				helper.innerJoin((SortMergeInnerJoinIterator) joinIterator2, false);
			} else {
				joinIterator1 = new SortMergeInnerJoinIterator(
						serializer2, serializer1, projection2, projection1,
						keyComparator, iterator2, iterator1, newBuffer(serializer1), filterNulls);
				helper.innerJoin((SortMergeInnerJoinIterator) joinIterator1, true);
			}
		} else if (type.equals(SortMergeJoinType.LEFT)) {
			joinIterator2 = new SortMergeOneSideOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			helper.oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator2, false, rightNullRow);
		} else if (type.equals(SortMergeJoinType.RIGHT)) {
			joinIterator1 = new SortMergeOneSideOuterJoinIterator(
					serializer2, serializer1, projection2, projection1,
					keyComparator, iterator2, iterator1, newBuffer(serializer1), filterNulls);
			helper.oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator1, true, leftNullRow);
		} else if (type.equals(SortMergeJoinType.FULL)) {
			fullOuterJoinIterator = new SortMergeFullOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2,
					newBuffer(serializer1), newBuffer(serializer2), filterNulls);
			helper.fullOuterJoin(fullOuterJoinIterator);
		} else if (type.equals(SortMergeJoinType.SEMI)) {
			joinIterator2 = new SortMergeInnerJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			while (((SortMergeInnerJoinIterator) joinIterator2).nextInnerJoin()) {
				BaseRow probeRow = joinIterator2.getProbeRow();
				boolean matched = false;
				try (ResettableExternalBuffer.BufferIterator iter = joinIterator2.getMatchBuffer().newIterator()) {
					while (iter.advanceNext()) {
						BaseRow row = iter.getRow();
						if (condFunc.apply(probeRow, row)) {
							matched = true;
							break;
						}
					}
				}
				if (matched) {
					collector.collect(probeRow);
				}
			}
		} else if (type.equals(SortMergeJoinType.ANTI)) {
			joinIterator2 = new SortMergeOneSideOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			while (((SortMergeOneSideOuterJoinIterator) joinIterator2).nextOuterJoin()) {
				BaseRow probeRow = joinIterator2.getProbeRow();
				ResettableExternalBuffer matchBuffer = joinIterator2.getMatchBuffer();
				boolean matched = false;
				if (matchBuffer != null) {
					try (ResettableExternalBuffer.BufferIterator iter = matchBuffer.newIterator()) {
						while (iter.advanceNext()) {
							BaseRow row = iter.getRow();
							if (condFunc.apply(probeRow, row)) {
								matched = true;
								break;
							}
						}
					}
				}
				if (!matched) {
					collector.collect(probeRow);
				}
			}
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}

	private ResettableExternalBuffer newBuffer(BinaryRowSerializer serializer) throws MemoryAllocationException {
		List<MemorySegment> externalBufferSegments = memManager.allocatePages(
				this.getContainingTask(), (int) (externalBufferMemory / memManager.getPageSize()));
		return new ResettableExternalBuffer(memManager, ioManager, externalBufferSegments, serializer,
				false /* we don't use newIterator(int beginRow), so don't need use this optimization*/);
	}

	private boolean isAllFinished() {
		return isFinished[0] && isFinished[1];
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (this.sorter1 != null) {
			this.sorter1.close();
		}
		if (this.sorter2 != null) {
			this.sorter2.close();
		}
		if (this.joinIterator1 != null) {
			this.joinIterator1.close();
		}
		if (this.joinIterator2 != null) {
			this.joinIterator2.close();
		}
		if (this.fullOuterJoinIterator != null) {
			this.fullOuterJoinIterator.close();
		}
	}

	/**
	 * Join type for sort merge join.
	 */
	public enum SortMergeJoinType {
		INNER, LEFT, RIGHT, FULL, SEMI, ANTI
	}
}
