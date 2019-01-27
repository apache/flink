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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
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

import java.util.List;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation that realizes the joining through a sort-merge join strategy.
 */
public class SortMergeJoinOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long reservedSortMemory1;
	private final long maxSortMemory1;
	private final long reservedSortMemory2;
	private final long maxSortMemory2;
	private final long perRequestMemory;
	private final long externalBufferMemory;
	private final FlinkJoinRelType type;
	private final boolean leftIsSmaller;

	// generated code to cook
	private GeneratedJoinConditionFunction condFuncCode;
	private GeneratedProjection projectionCode1;
	private GeneratedProjection projectionCode2;
	private GeneratedSorter gSorter1;
	private GeneratedSorter gSorter2;
	private GeneratedSorter keyGSorter;
	private final boolean[] filterNulls;

	private transient CookedClasses classes;

	private transient Configuration conf;
	private transient MemoryManager memManager;
	private transient IOManager ioManager;
	private transient TypeSerializer<BaseRow> inputSerializer1;
	private transient TypeSerializer<BaseRow> inputSerializer2;
	private transient BinaryRowSerializer serializer1;
	private transient BinaryRowSerializer serializer2;
	private transient BinaryExternalSorter sorter1;
	private transient BinaryExternalSorter sorter2;
	private transient SortMergeJoinIterator joinIterator1;
	private transient SortMergeJoinIterator joinIterator2;
	private transient SortMergeFullOuterJoinIterator fullOuterJoinIterator;
	private transient Collector<BaseRow> collector;
	private transient boolean[] isFinished;
	private transient JoinConditionFunction condFunc;
	private transient RecordComparator keyComparator;

	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;

	private transient SortMergeJoinHelper helper;

	public SortMergeJoinOperator(
			long reservedSortMemory, long maxSortMemory,
			long perRequestMemory, long externalBufferMemory, FlinkJoinRelType type, boolean leftIsSmaller,
			GeneratedJoinConditionFunction condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedSorter gSorter1, GeneratedSorter gSorter2, GeneratedSorter keyGSorter,
			boolean[] filterNulls) {
		this(reservedSortMemory, maxSortMemory, reservedSortMemory, maxSortMemory, perRequestMemory,
				externalBufferMemory, type, leftIsSmaller, condFuncCode, projectionCode1, projectionCode2, gSorter1,
				gSorter2, keyGSorter, filterNulls);
	}

	public SortMergeJoinOperator(
			long reservedSortMemory1, long maxSortMemory1,
			long reservedSortMemory2, long maxSortMemory2,
			long perRequestMemory, long externalBufferMemory, FlinkJoinRelType type, boolean leftIsSmaller,
			GeneratedJoinConditionFunction condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedSorter gSorter1, GeneratedSorter gSorter2, GeneratedSorter keyGSorter,
			boolean[] filterNulls) {
		this.reservedSortMemory1 = reservedSortMemory1;
		this.maxSortMemory1 = maxSortMemory1;
		this.reservedSortMemory2 = reservedSortMemory2;
		this.maxSortMemory2 = maxSortMemory2;
		this.perRequestMemory = perRequestMemory;
		this.externalBufferMemory = externalBufferMemory;
		this.type = type;
		this.leftIsSmaller = leftIsSmaller;
		this.condFuncCode = condFuncCode;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.gSorter1 = checkNotNull(gSorter1);
		this.gSorter2 = checkNotNull(gSorter2);
		this.keyGSorter = checkNotNull(keyGSorter);
		this.filterNulls = filterNulls;
	}

	@Override
	public void open() throws Exception {
		super.open();

		conf = getSqlConf();

		// code gen classes.
		this.classes = cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());

		isFinished = new boolean[2];
		isFinished[0] = false;
		isFinished[1] = false;

		collector = new StreamRecordCollector<>(output);

		this.inputSerializer1 = getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		this.serializer1 =
				new BinaryRowSerializer(((AbstractRowSerializer) inputSerializer1).getTypes());

		this.inputSerializer2 = getOperatorConfig().getTypeSerializerIn2(getUserCodeClassloader());
		this.serializer2 =
				new BinaryRowSerializer(((AbstractRowSerializer) inputSerializer2).getTypes());

		this.memManager = this.getContainingTask().getEnvironment().getMemoryManager();
		this.ioManager = this.getContainingTask().getEnvironment().getIOManager();

		initSorter();
		initKeyComparator();

		this.condFunc = classes.condFuncClass.newInstance();

		this.leftNullRow = new GenericRow(serializer1.getNumFields());
		this.rightNullRow = new GenericRow(serializer2.getNumFields());
		JoinedRow joinedRow = new JoinedRow();

		this.helper = new SortMergeJoinHelper(collector, condFunc, leftNullRow, rightNullRow, joinedRow);

		condFuncCode = null;
		keyGSorter = null;
		projectionCode1 = null;
		projectionCode2 = null;
		gSorter1 = null;
		gSorter2 = null;

		getMetricGroup().gauge("memoryUsedSizeInBytes",
			(Gauge<Long>) () -> sorter1.getUsedMemoryInBytes() + sorter2.getUsedMemoryInBytes());

		getMetricGroup().gauge("numSpillFiles",
			(Gauge<Long>) () -> sorter1.getNumSpillFiles() + sorter2.getNumSpillFiles());

		getMetricGroup().gauge("spillInBytes",
			(Gauge<Long>) () -> sorter1.getSpillInBytes() + sorter2.getSpillInBytes());
	}

	private void initSorter() throws Exception {
		// sorter1
		NormalizedKeyComputer computer1 = classes.computerClass1.newInstance();
		RecordComparator comparator1 = classes.comparatorClass1.newInstance();
		computer1.init(gSorter1.serializers(), gSorter1.comparators());
		comparator1.init(gSorter1.serializers(), gSorter1.comparators());
		this.sorter1 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory1, maxSortMemory1, perRequestMemory,
				ioManager, inputSerializer1, serializer1, computer1, comparator1, conf);
		this.sorter1.startThreads();

		// sorter2
		NormalizedKeyComputer computer2 = classes.computerClass2.newInstance();
		RecordComparator comparator2 = classes.comparatorClass2.newInstance();
		computer2.init(gSorter2.serializers(), gSorter2.comparators());
		comparator2.init(gSorter2.serializers(), gSorter2.comparators());
		this.sorter2 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory2, maxSortMemory2, perRequestMemory,
				ioManager, inputSerializer2, serializer2, computer2, comparator2, conf);
		this.sorter2.startThreads();
	}

	private void initKeyComparator() throws Exception {
		keyComparator = classes.keyComparatorClass.newInstance();
		keyComparator.init(keyGSorter.serializers(), keyGSorter.comparators());
	}

	protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
		return new CookedClasses(
				compile(cl, condFuncCode.name(), condFuncCode.code()),
				compile(cl, keyGSorter.comparator().name(), keyGSorter.comparator().code()),
				compile(cl, projectionCode1.name(), projectionCode1.code()),
				compile(cl, projectionCode2.name(), projectionCode2.code()),
				compile(cl, gSorter1.computer().name(), gSorter1.computer().code()),
				compile(cl, gSorter2.computer().name(), gSorter2.computer().code()),
				compile(cl, gSorter1.comparator().name(), gSorter1.comparator().code()),
				compile(cl, gSorter2.comparator().name(), gSorter2.comparator().code())
		);
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		this.sorter1.write(element.getValue());
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		this.sorter2.write(element.getValue());
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {
		isFinished[0] = true;
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	@Override
	public void endInput2() throws Exception {
		isFinished[1] = true;
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	private void doSortMergeJoin() throws Exception {

		Projection projection1 = classes.projectionClass1.newInstance();
		Projection projection2 = classes.projectionClass2.newInstance();
		MutableObjectIterator iterator1 = sorter1.getIterator();
		MutableObjectIterator iterator2 = sorter2.getIterator();

		if (type.equals(FlinkJoinRelType.INNER)) {
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
		} else if (type.equals(FlinkJoinRelType.LEFT)) {
			joinIterator2 = new SortMergeOneSideOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			helper.oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator2, false, rightNullRow);
		} else if (type.equals(FlinkJoinRelType.RIGHT)) {
			joinIterator1 = new SortMergeOneSideOuterJoinIterator(
					serializer2, serializer1, projection2, projection1,
					keyComparator, iterator2, iterator1, newBuffer(serializer1), filterNulls);
			helper.oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator1, true, leftNullRow);
		} else if (type.equals(FlinkJoinRelType.FULL)) {
			fullOuterJoinIterator = new SortMergeFullOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2,
					newBuffer(serializer1), newBuffer(serializer2), filterNulls);
			helper.fullOuterJoin(fullOuterJoinIterator);
		} else if (type.equals(FlinkJoinRelType.SEMI)) {
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
		} else if (type.equals(FlinkJoinRelType.ANTI)) {
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
			throw new RuntimeException("Not support yet!");
		}
	}

	private ResettableExternalBuffer newBuffer(BinaryRowSerializer serializer) throws MemoryAllocationException {
		List<MemorySegment> externalBufferSegments = memManager.allocatePages(
				this.getContainingTask(), (int) (externalBufferMemory / memManager.getPageSize()));
		return new ResettableExternalBuffer(memManager, ioManager, externalBufferSegments, serializer);
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
	 * Generated classes.
	 */
	protected static class CookedClasses {

		protected CookedClasses(
				Class<JoinConditionFunction> condFuncClass,
				Class<RecordComparator> keyComparatorClass,
				Class<Projection> projectionClass1,
				Class<Projection> projectionClass2,
				Class<NormalizedKeyComputer> computerClass1,
				Class<NormalizedKeyComputer> computerClass2,
				Class<RecordComparator> comparatorClass1,
				Class<RecordComparator> comparatorClass2) {
			this.condFuncClass = condFuncClass;
			this.keyComparatorClass = keyComparatorClass;
			this.projectionClass1 = projectionClass1;
			this.projectionClass2 = projectionClass2;
			this.computerClass1 = computerClass1;
			this.computerClass2 = computerClass2;
			this.comparatorClass1 = comparatorClass1;
			this.comparatorClass2 = comparatorClass2;
		}

		protected final Class<JoinConditionFunction> condFuncClass;
		protected final Class<RecordComparator> keyComparatorClass;
		protected final Class<Projection> projectionClass1;
		protected final Class<Projection> projectionClass2;
		protected final Class<NormalizedKeyComputer> computerClass1;
		protected final Class<NormalizedKeyComputer> computerClass2;
		protected final Class<RecordComparator> comparatorClass1;
		protected final Class<RecordComparator> comparatorClass2;
	}
}
