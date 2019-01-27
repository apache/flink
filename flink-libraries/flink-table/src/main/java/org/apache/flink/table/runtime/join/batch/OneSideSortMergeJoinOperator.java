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

package org.apache.flink.table.runtime.join.batch;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;

/**
 * An implementation that realizes the joining through a sort-merge join strategy.
 * This operator only sorts one input, so another input should be sorted on the same key in advance.
 */
public class OneSideSortMergeJoinOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
	implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long reservedSortMemory;
	private final long maxSortMemory;
	private final long perRequestMemory;
	private final long probeBufferMemory;
	private final long joinBufferMemory;
	private final FlinkJoinRelType type;
	private final boolean leftNeedsSort;
	private final boolean[] filterNulls;

	private GeneratedJoinConditionFunction condFuncCode;
	private final GeneratedProjection projectionCode1;
	private final GeneratedProjection projectionCode2;
	private final GeneratedSorter gSorter;
	private final GeneratedSorter keyGSorter;

	private transient CookedClasses classes;

	private transient MemoryManager memManager;
	private transient IOManager ioManager;
	private transient AbstractRowSerializer inputSerializer1;
	private transient AbstractRowSerializer inputSerializer2;
	private transient BinaryRowSerializer serializer1;
	private transient BinaryRowSerializer serializer2;

	private transient BinaryExternalSorter sorter;
	private transient BaseRow probeRow;
	private transient ResettableExternalBuffer probeBuffer;
	private transient ProbeIterator probeIter;
	private transient SortMergeJoinIterator joinIterator;

	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;

	private transient boolean isFinished1;
	private transient boolean isFinished2;

	private transient SortMergeJoinHelper helper;

	public OneSideSortMergeJoinOperator(
		long reservedSortMemory,
		long maxSortMemory,
		long perRequestMemory,
		long probeBufferMemory,
		long joinBufferMemory,
		FlinkJoinRelType type,
		boolean leftNeedsSort,
		GeneratedJoinConditionFunction condFuncCode,
		GeneratedProjection projectionCode1,
		GeneratedProjection projectionCode2,
		GeneratedSorter gSorter,
		GeneratedSorter keyGSorter,
		boolean[] filterNulls) {

		if (type != FlinkJoinRelType.INNER
			&& type != FlinkJoinRelType.LEFT
			&& type != FlinkJoinRelType.RIGHT) {
			throw new RuntimeException("One side sort merge join operator only supports " +
				"inner/left outer/right outer join currently.");
		}

		LOG.info(
			"Initializing one side sort merge join operator...\n" +
				"leftNeedsSort = " + leftNeedsSort + ", " +
				"reservedSortMemory = " + reservedSortMemory + ", " +
				"preferredSortMemory = " + maxSortMemory + ", " +
				"perRequestMemory = " + perRequestMemory + ", " +
				"probeBufferMemory = " + probeBufferMemory + ", " +
				"joinBufferMemory = " + joinBufferMemory);

		this.reservedSortMemory = reservedSortMemory;
		this.maxSortMemory = maxSortMemory;
		this.perRequestMemory = perRequestMemory;
		this.probeBufferMemory = probeBufferMemory;
		this.joinBufferMemory = joinBufferMemory;
		this.type = type;
		this.leftNeedsSort = leftNeedsSort;
		this.filterNulls = filterNulls;

		this.condFuncCode = condFuncCode;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.gSorter = gSorter;
		this.keyGSorter = keyGSorter;
	}

	@Override
	public void open() throws Exception {
		super.open();

		isFinished1 = false;
		isFinished2 = false;

		classes = cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());

		Collector<BaseRow> collector = new StreamRecordCollector<>(output);

		inputSerializer1 = (AbstractRowSerializer) getOperatorConfig()
			.getTypeSerializerIn1(getUserCodeClassloader());
		serializer1 = new BinaryRowSerializer(inputSerializer1.getTypes());

		inputSerializer2 = (AbstractRowSerializer) getOperatorConfig()
			.getTypeSerializerIn2(getUserCodeClassloader());
		serializer2 = new BinaryRowSerializer(inputSerializer2.getTypes());

		memManager = this.getContainingTask().getEnvironment().getMemoryManager();
		ioManager = this.getContainingTask().getEnvironment().getIOManager();

		JoinConditionFunction condFunc = classes.condFuncClass.newInstance();

		leftNullRow = new GenericRow(serializer1.getNumFields());
		rightNullRow = new GenericRow(serializer2.getNumFields());
		JoinedRow joinedRow = new JoinedRow();

		helper =
			new SortMergeJoinHelper(collector, condFunc, leftNullRow, rightNullRow, joinedRow);

		initSorter();
		probeBuffer =
			newBuffer(probeBufferMemory, leftNeedsSort ? inputSerializer2 : inputSerializer1);

		initGauge();
	}

	protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
		return new CookedClasses(
			compile(cl, condFuncCode.name(), condFuncCode.code()),
			compile(cl, keyGSorter.comparator().name(), keyGSorter.comparator().code()),
			compile(cl, projectionCode1.name(), projectionCode1.code()),
			compile(cl, projectionCode2.name(), projectionCode2.code()),
			compile(cl, gSorter.computer().name(), gSorter.computer().code()),
			compile(cl, gSorter.comparator().name(), gSorter.comparator().code())
		);
	}

	private void initGauge() {
		getMetricGroup().gauge(
			"memoryUsedSizeInBytes",
			(Gauge<Long>) () -> sorter.getUsedMemoryInBytes() + probeBuffer.getUsedMemoryInBytes());

		getMetricGroup().gauge(
			"numSpillFiles",
			(Gauge<Long>) () -> sorter.getNumSpillFiles() + probeBuffer.getNumSpillFiles());

		getMetricGroup().gauge(
			"spillInBytes",
			(Gauge<Long>) () -> sorter.getSpillInBytes() + probeBuffer.getSpillInBytes());
	}

	private void initSorter() throws Exception {
		NormalizedKeyComputer computer = classes.computerClass.newInstance();
		computer.init(gSorter.serializers(), gSorter.comparators());

		RecordComparator comparator = classes.comparatorClass.newInstance();
		comparator.init(gSorter.serializers(), gSorter.comparators());

		AbstractRowSerializer inputSerializer;
		BinaryRowSerializer serializer;
		if (leftNeedsSort) {
			inputSerializer = inputSerializer1;
			serializer = serializer1;
		} else {
			inputSerializer = inputSerializer2;
			serializer = serializer2;
		}

		sorter = new BinaryExternalSorter(this.getContainingTask(),
			memManager, reservedSortMemory, maxSortMemory, perRequestMemory,
			ioManager, inputSerializer, serializer, computer, comparator, getSqlConf());
		this.sorter.startThreads();
	}

	private void initJoinIterator() throws Exception {
		Projection projection1 = classes.projectionClass1.newInstance();
		Projection projection2 = classes.projectionClass2.newInstance();

		probeIter = new ProbeIterator();
		MutableObjectIterator<BinaryRow> sortIter = sorter.getIterator();

		RecordComparator keyComparator = classes.keyComparatorClass.newInstance();
		keyComparator.init(keyGSorter.serializers(), keyGSorter.comparators());

		if (type == FlinkJoinRelType.INNER) {
			if (leftNeedsSort) {
				joinIterator = new SortMergeInnerJoinIterator(
					serializer2, serializer1, projection2, projection1, keyComparator,
					probeIter, sortIter, newBuffer(joinBufferMemory, serializer1), filterNulls);
			} else {
				joinIterator = new SortMergeInnerJoinIterator(
					serializer1, serializer2, projection1, projection2, keyComparator,
					probeIter, sortIter, newBuffer(joinBufferMemory, serializer2), filterNulls);
			}
		} else if (type == FlinkJoinRelType.LEFT) {
			joinIterator = new SortMergeOneSideOuterJoinIterator(
				serializer1, serializer2, projection1, projection2, keyComparator,
				probeIter, sortIter, newBuffer(joinBufferMemory, serializer2), filterNulls);
		} else if (type == FlinkJoinRelType.RIGHT) {
			joinIterator = new SortMergeOneSideOuterJoinIterator(
				serializer2, serializer1, projection2, projection1, keyComparator,
				probeIter, sortIter, newBuffer(joinBufferMemory, serializer1), filterNulls);
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> record) throws Exception {
		if (leftNeedsSort) {
			sorter.write(record.getValue());
		} else {
			BaseRow row = record.getValue();
			if (isFinished2) {
				probeRow = row;
				runJoin();
			} else {
				probeBuffer.add(row);
			}
		}
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> record) throws Exception {
		if (leftNeedsSort) {
			BaseRow row = record.getValue();
			if (isFinished1) {
				probeRow = row;
				runJoin();
			} else {
				probeBuffer.add(row);
			}
		} else {
			sorter.write(record.getValue());
		}
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {
		isFinished1 = true;
		if (leftNeedsSort) {
			initJoinIterator();
			runJoin();
		}
	}

	@Override
	public void endInput2() throws Exception {
		isFinished2 = true;
		if (!leftNeedsSort) {
			initJoinIterator();
			runJoin();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (probeIter != null) {
			probeIter.close();
		}

		if (probeBuffer != null) {
			probeBuffer.close();
		}

		if (sorter != null) {
			sorter.close();
		}

		if (joinIterator != null) {
			joinIterator.close();
		}
	}

	private void runJoin() throws Exception {
		if (type == FlinkJoinRelType.INNER) {
			helper.innerJoin((SortMergeInnerJoinIterator) joinIterator, leftNeedsSort);
		} else if (type == FlinkJoinRelType.LEFT) {
			if (leftNeedsSort) {
				throw new RuntimeException("Not support yet!");
			} else {
				helper.oneSideOuterJoin(
					(SortMergeOneSideOuterJoinIterator) joinIterator, false, rightNullRow);
			}
		} else if (type == FlinkJoinRelType.RIGHT) {
			if (leftNeedsSort) {
				helper.oneSideOuterJoin(
					(SortMergeOneSideOuterJoinIterator) joinIterator, true, leftNullRow);
			} else {
				throw new RuntimeException("Not support yet!");
			}
		}
	}

	private ResettableExternalBuffer newBuffer(
		long memorySize, AbstractRowSerializer serializer) throws MemoryAllocationException {
		List<MemorySegment> externalBufferSegments = memManager.allocatePages(
			this.getContainingTask(), (int) (memorySize / memManager.getPageSize()));
		return new ResettableExternalBuffer(memManager, ioManager, externalBufferSegments, serializer);
	}

	/**
	 * Generated classes.
	 */
	protected static class CookedClasses {

		protected CookedClasses(
			Class<JoinConditionFunction> condFuncClass,
			Class<RecordComparator> keyComparatorClass,
			Class<Projection> projectionClass1,
			Class<Projection> projectionClass2,
			Class<NormalizedKeyComputer> computerClass,
			Class<RecordComparator> comparatorClass) {
			this.condFuncClass = condFuncClass;
			this.keyComparatorClass = keyComparatorClass;
			this.projectionClass1 = projectionClass1;
			this.projectionClass2 = projectionClass2;
			this.computerClass = computerClass;
			this.comparatorClass = comparatorClass;
		}

		protected final Class<JoinConditionFunction> condFuncClass;
		protected final Class<RecordComparator> keyComparatorClass;
		protected final Class<Projection> projectionClass1;
		protected final Class<Projection> projectionClass2;
		protected final Class<NormalizedKeyComputer> computerClass;
		protected final Class<RecordComparator> comparatorClass;
	}

	private class ProbeIterator implements MutableObjectIterator<BaseRow> {

		private ResettableExternalBuffer.BufferIterator bufferIterator;

		private ProbeIterator() {
			bufferIterator = probeBuffer.newIterator();
		}

		@Override
		public BaseRow next(BaseRow reuse) throws IOException {
			return next();
		}

		@Override
		public BaseRow next() throws IOException {
			if (bufferIterator.hasNext()) {
				bufferIterator.advanceNext();
				return bufferIterator.getRow();
			} else {
				if (probeRow == null) {
					return null;
				}

				BaseRow row = probeRow;
				probeRow = null;
				return row;
			}
		}

		public void close() {
			bufferIterator.close();
		}
	}
}
