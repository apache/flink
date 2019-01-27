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
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.codehaus.commons.compiler.CompileException;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;

/**
 * An implementation that realizes the joining through a merge join strategy.
 * This requires the two input to be sorted on the same key in advance.
 *
 * <p>This operator maintains one buffer for each input.
 * The `current` base row in each buffer serves as a pointer to the currently compared row.</p>
 *
 * <p>This operator contains two stage, the "find" stage and the "merge" stage.</p>
 *
 * <p>1. The "find" stage</p>
 *
 * <p>The goal of this stage is to find two rows with the same join key in each buffer.</p>
 *
 * <p>When receiving a row from the input, we store the row into the corresponding buffer,
 * and compare the join key of the `current` rows in each buffer.
 * The buffer with the smaller join key should advance its `current` row.
 * If the join keys of the two `current` rows are the same, we will move into the "merge" stage.</p>
 *
 * <p>2. The "merge" stage</p>
 *
 * <p>The goal of this stage is to join the rows with the same join key discovered in the "find" stage.</p>
 *
 * <p>We will select a buffer as the build buffer, and the other buffer as the probe buffer.
 * We will then wait for all rows with the same join key to arrive at the build buffer.
 * (If a row arrives at the probe buffer, we will of course store it,
 * but no further operations will be done until all build rows are received).
 * After that, we will iterate through the probe buffer and check if the rows can be joined.</p>
 */
public class MergeJoinOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
	implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long leftBufferMemory;
	private final long rightBufferMemory;
	private final FlinkJoinRelType type;

	private final GeneratedJoinConditionFunction condFuncCode;
	private final GeneratedProjection projectionCode1;
	private final GeneratedProjection projectionCode2;
	private final GeneratedSorter keyGSorter;

	private transient JoinConditionFunction condFunc;
	private transient RecordComparator keyComparator;

	private transient Collector<BaseRow> collector;
	private transient boolean isFinished1;
	private transient boolean isFinished2;

	private transient AbstractRowSerializer<BaseRow> serializer1;
	private transient AbstractRowSerializer<BaseRow> serializer2;

	private transient WrappedBuffer buffer1;
	private transient WrappedBuffer buffer2;

	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;
	private transient JoinedRow joinedRow;

	private transient boolean isFindStage;
	private transient boolean advance1;
	private transient boolean advance2;

	private transient BinaryRow mergeKey;
	private transient int mergeCount1;
	private transient int mergeCount2;
	private transient BitSet mergeBs1;
	private transient BitSet mergeBs2;
	private transient boolean leftIsBuild;

	private transient MemoryManager memManager;
	private transient IOManager ioManager;

	private final int[] nullFilterKeys;
	private final boolean nullSafe;
	private final boolean filterAllNulls;

	public MergeJoinOperator(
		long leftBufferMemory,
		long rightBufferMemory,
		FlinkJoinRelType type,
		GeneratedJoinConditionFunction condFuncCode,
		GeneratedProjection projectionCode1,
		GeneratedProjection projectionCode2,
		GeneratedSorter keyGSorter,
		boolean[] filterNulls) {

		if (type != FlinkJoinRelType.INNER
			&& type != FlinkJoinRelType.LEFT
			&& type != FlinkJoinRelType.RIGHT
			&& type != FlinkJoinRelType.FULL) {
			throw new RuntimeException(
				"Merge join operator only supports inner/left outer/right outer/full outer join currently.");
		}

		LOG.info(
			"Initializing merge join operator...\n" +
				"leftBufferMemory = " + leftBufferMemory + ", " +
				"rightBufferMemory = " + rightBufferMemory);

		this.leftBufferMemory = leftBufferMemory;
		this.rightBufferMemory = rightBufferMemory;
		this.type = type;

		this.condFuncCode = condFuncCode;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.keyGSorter = keyGSorter;

		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNulls.length;
	}

	@Override
	public void open() throws Exception {
		super.open();

		isFinished1 = false;
		isFinished2 = false;

		collector = new StreamRecordCollector<>(output);

		serializer1 = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		serializer2 = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn2(getUserCodeClassloader());

		leftNullRow = new GenericRow(serializer1.getNumFields());
		rightNullRow = new GenericRow(serializer2.getNumFields());
		joinedRow = new JoinedRow();

		CookedClasses classes = cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());
		condFunc = classes.condFuncClass.newInstance();
		keyComparator = classes.keyComparatorClass.newInstance();
		keyComparator.init(keyGSorter.serializers(), keyGSorter.comparators());

		memManager = getContainingTask().getEnvironment().getMemoryManager();
		ioManager = getContainingTask().getEnvironment().getIOManager();

		Projection<BaseRow, BinaryRow> projection1 = classes.projectionClass1.newInstance();
		Projection<BaseRow, BinaryRow> projection2 = classes.projectionClass2.newInstance();

		int pageNum1 = (int) (leftBufferMemory / memManager.getPageSize());
		List<MemorySegment> mem1 = memManager.allocatePages(getContainingTask(), pageNum1);
		buffer1 = new WrappedBuffer(mem1, serializer1, projection1);

		int pageNum2 = (int) (rightBufferMemory / memManager.getPageSize());
		List<MemorySegment> mem2 = memManager.allocatePages(getContainingTask(), pageNum2);
		buffer2 = new WrappedBuffer(mem2, serializer2, projection2);

		initGauge();

		initJoin();
	}

	protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
		return new CookedClasses(
			compile(cl, condFuncCode.name(), condFuncCode.code()),
			compile(cl, keyGSorter.comparator().name(), keyGSorter.comparator().code()),
			compile(cl, projectionCode1.name(), projectionCode1.code()),
			compile(cl, projectionCode2.name(), projectionCode2.code())
		);
	}

	private void initJoin() {
		advance1 = true;
		advance2 = true;
		isFindStage = true;

		if (type.isLeftOuter()) {
			mergeBs1 = new BitSet();
		}
		if (type.isRightOuter()) {
			mergeBs2 = new BitSet();
		}
	}

	private void initGauge() {
		getMetricGroup().gauge("memoryUsedSizeInBytes",
			(Gauge<Long>) () -> buffer1.getUsedMemoryInBytes() + buffer2.getUsedMemoryInBytes());

		getMetricGroup().gauge(
			"numSpillFiles",
			(Gauge<Integer>) () -> buffer1.getNumSpillFiles() + buffer2.getNumSpillFiles());

		getMetricGroup().gauge(
			"spillInBytes",
			(Gauge<Long>) () -> buffer1.getSpillInBytes() + buffer2.getSpillInBytes());
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> record) throws Exception {
		buffer1.add(record.getValue());
		runJoin();
		// must materialize cache immediately after the operation,
		// because the record will be reused and the data it contains will be changed
		buffer1.materializeCache();
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> record) throws Exception {
		buffer2.add(record.getValue());
		runJoin();
		// must materialize cache immediately after the operation,
		// because the record will be reused and the data it contains will be changed
		buffer2.materializeCache();
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {
		buffer1.add(BinaryRowUtil.EMPTY_ROW);
		isFinished1 = true;

		if (isAllFinished()) {
			runJoin();
		}
	}

	@Override
	public void endInput2() throws Exception {
		buffer2.add(BinaryRowUtil.EMPTY_ROW);
		isFinished2 = true;

		if (isAllFinished()) {
			runJoin();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (buffer1 != null) {
			buffer1.close();
		}

		if (buffer2 != null) {
			buffer2.close();
		}
	}

	private boolean isAllFinished() {
		return isFinished1 && isFinished2;
	}

	private void runJoin() throws Exception {
		boolean stepResult = true;
		while (stepResult) {
			if (isFindStage) {
				stepResult = runFindStep();
			} else {
				stepResult = runMergeStep();
			}
		}
	}

	/**
	 * Do one step of the "find" stage.
	 *
	 * <p>It will advance the current row of the buffer with the smaller join key,
	 * and then compare the join keys of the current rows in two buffers.</p>
	 */
	private boolean runFindStep() {
		if ((advance1 && !buffer1.hasNext())
			|| (advance2 && !buffer2.hasNext())) {
			return false;
		}

		BaseRow row1 = advance1 ? buffer1.nextRow() : buffer1.current;
		BaseRow row2 = advance2 ? buffer2.nextRow() : buffer2.current;
		boolean end1 = isEndRow(row1);
		boolean end2 = isEndRow(row2);

		int cmp;
		if (end1 && end2) {
			return false;
		} else if (end1) {
			cmp = 1;
		} else if (end2) {
			cmp = -1;
		} else if (buffer1.currentKeyShouldFilter) {
			cmp = -1;
		} else if (buffer2.currentKeyShouldFilter) {
			cmp = 1;
		} else {
			BinaryRow key1 = buffer1.currentKey;
			BinaryRow key2 = buffer2.currentKey;
			cmp = keyComparator.compare(key1, key2);
		}

		if (cmp < 0) {
			// `row1` is smaller, so buffer1 should advance
			advance1 = true;
			advance2 = false;
			if (type.isLeftOuter()) {
				collect(row1, rightNullRow);
			}
			buffer1.discard();
		} else if (cmp > 0) {
			// `row2` is smaller, so buffer2 should advance
			advance1 = false;
			advance2 = true;
			if (type.isRightOuter()) {
				collect(leftNullRow, row2);
			}
			buffer2.discard();
		} else {
			// same join key found
			// find stage end, go to merge stage
			initMergeStage();
		}

		return true;
	}

	private void initMergeStage() {
		isFindStage = false;

		// selects the build buffer
		// only after all bulid rows in the build buffer arrive,
		// can we iterate through the probe buffer and check for joins.
		if (leftIsBuild = buffer1.isBuild()) {
			advance1 = true;
			advance2 = false;
			buffer1.enterBuildMode();
		} else {
			advance1 = false;
			advance2 = true;
			buffer2.enterBuildMode();
		}

		mergeKey = buffer1.currentKey.copy();

		mergeCount1 = 0;
		mergeCount2 = 0;

		if (mergeBs1 != null) {
			mergeBs1.clear();
		}
		if (mergeBs2 != null) {
			mergeBs2.clear();
		}
	}

	/**
	 * Do one step of the "merge" stage.
	 *
	 * <p>We will first try to collect the build rows in the build buffer,
	 * until a row with different join key is received.
	 * We will then iterate through the probe buffer and check for joins.</p>
	 */
	private boolean runMergeStep() throws Exception {
		if (advance1 && buffer1.hasNext()) {
			mergeCount1++;
			if (leftIsBuild) {
				advance1 = checkNextRowIsSameKey(buffer1);
				if (!advance1 && leftIsBuild) {
					// A different join key is read from the build buffer.
					// This means we've received all build rows, so the probe side can go on.
					// At this time, all build rows are in `externalBuffer` of the build buffer,
					// and the `cache` in build buffer contains the first row with different join key.
					advance2 = true;
				}
			} else {
				advance1 = joinCurrentProbeRow(buffer1, buffer2, mergeCount2);
			}
			return true;
		} else if (advance2 && buffer2.hasNext()) {
			mergeCount2++;
			if (leftIsBuild) {
				advance2 = joinCurrentProbeRow(buffer2, buffer1, mergeCount1);
			} else {
				advance2 = checkNextRowIsSameKey(buffer2);
				if (!advance2 && !leftIsBuild) {
					// A different join key is read from the build buffer.
					// This means we've received all build rows, so the probe side can go on.
					// At this time, all build rows are in `externalBuffer` of the build buffer,
					// and the `cache` in build buffer contains the first row with different join key.
					advance1 = true;
				}
			}
			return true;
		} else if (!advance1 && !advance2) {
			// merge stage end, go to find stage
			endMergeStage();
			return true;
		} else {
			return false;
		}
	}

	private boolean checkNextRowIsSameKey(WrappedBuffer buffer) {
		BaseRow row = buffer.nextRow();
		if (isEndRow(row)) {
			return false;
		} else {
			BinaryRow key = buffer.currentKey;
			// NOTE
			// Here we suppose that
			// `keyComparator.compare(mergeKey, key) == 0` is the same as `mergeKey.equals(key)`.
			//
			// `mergeKey.equals(key)` is faster, because `mergeKey` is a binary row,
			// and the equality is compared by binary blocks, not by columns.
			//
			// If they are not the same in the future, please modify this.
			return !buffer.currentKeyShouldFilter && mergeKey.equals(key);
		}
	}

	/**
	 * Check for joins using the current probe row and all build rows.
	 * After checking, we will advance the current probe row,
	 * and if the join key of the next row is not the same, the "merge" stage will end.
	 */
	private boolean joinCurrentProbeRow(
		WrappedBuffer probeBuffer, WrappedBuffer buildBuffer, int buildCount) throws Exception {

		BaseRow probeRow = probeBuffer.current;
		boolean matched = false;

		ResettableExternalBuffer.BufferIterator buildIter = buildBuffer.externalIterator;
		buildIter.reset();

		for (int index = 0; index < buildCount; index++) {
			boolean result = buildIter.advanceNext();
			Preconditions.checkState(result, "There is no next row in build buffer. This is a bug.");

			BinaryRow buildRow = buildIter.getRow();
			if (leftIsBuild) {
				if (condFunc.apply(buildRow, probeRow)) {
					matched = true;
					collect(buildRow, probeRow);
					if (mergeBs1 != null) {
						mergeBs1.set(index);
					}
				}
			} else {
				if (condFunc.apply(probeRow, buildRow)) {
					matched = true;
					collect(probeRow, buildRow);
					if (mergeBs2 != null) {
						mergeBs2.set(index);
					}
				}
			}
		}
		// After the iteration above,
		// the current row of `buildBuffer.externalIterator` is the last build row.

		// for the probe row of outer joins
		if (!matched) {
			if (mergeBs1 != null && !leftIsBuild) {
				collect(probeRow, rightNullRow);
			} else if (mergeBs2 != null && leftIsBuild) {
				collect(leftNullRow, probeRow);
			}
		}

		return checkNextRowIsSameKey(probeBuffer);
	}

	private void endMergeStage() throws IOException {
		if (leftIsBuild && buffer1.externalIterator.rowInSpill(buffer1.externalIterator.getBeginRow())) {
			LOG.warn("(In merge join operator) Build side iterator is in spilled file, " +
				"this may decrease performance.");
		} else if (!leftIsBuild && buffer2.externalIterator.rowInSpill(buffer2.externalIterator.getBeginRow())) {
			LOG.warn("(In merge join operator) Build side iterator is in spilled file, " +
				"this may decrease performance.");
		}

		// for the build rows of outer joins
		if (leftIsBuild) {
			if (mergeBs1 != null) {
				buffer1.externalIterator.reset();
				for (int i = 0; i < mergeCount1; i++) {
					buffer1.externalIterator.advanceNext();
					if (!mergeBs1.get(i)) {
						collect(buffer1.externalIterator.getRow(), rightNullRow);
					}
				}
			}
		} else {
			if (mergeBs2 != null) {
				buffer2.externalIterator.reset();
				for (int i = 0; i < mergeCount2; i++) {
					buffer2.externalIterator.advanceNext();
					if (!mergeBs2.get(i)) {
						collect(leftNullRow, buffer2.externalIterator.getRow());
					}
				}
			}
		}

		isFindStage = true;
		advance1 = false;
		advance2 = false;
		if (leftIsBuild) {
			buffer1.leaveBuildMode();
		} else {
			buffer2.leaveBuildMode();
		}
	}

	private void collect(BaseRow row1, BaseRow row2) {
		collector.collect(joinedRow.replace(row1, row2));
	}

	private static boolean isEndRow(BaseRow row) {
		return row.getArity() == 0;
	}

	private boolean shouldFilterNull(BinaryRow key) {
		return NullAwareJoinHelper.shouldFilter(nullSafe, filterAllNulls, nullFilterKeys, key);
	}

	/**
	 * Generated classes.
	 */
	protected static class CookedClasses {

		protected CookedClasses(
			Class<JoinConditionFunction> condFuncClass,
			Class<RecordComparator> keyComparatorClass,
			Class<Projection> projectionClass1,
			Class<Projection> projectionClass2) {
			this.condFuncClass = condFuncClass;
			this.keyComparatorClass = keyComparatorClass;
			this.projectionClass1 = projectionClass1;
			this.projectionClass2 = projectionClass2;
		}

		protected final Class<JoinConditionFunction> condFuncClass;
		protected final Class<RecordComparator> keyComparatorClass;
		protected final Class<Projection> projectionClass1;
		protected final Class<Projection> projectionClass2;
	}

	/**
	 * A helper buffer class for merge join operator.
	 *
	 * <p>This "buffer" contains a {@link BaseRow} and a real {@link ResettableExternalBuffer}.</p>
	 *
	 * <p>One can regard this buffer as a queue,
	 * where the head of the resettable external buffer is the head of the queue,
	 * and the base row is the tail of the queue.</p>
	 *
	 * <p>The base row is only a reference to the reusable inputs of the operator.
	 * If the content of the base row should be reserved for later usage,
	 * it must be materialized to the resettable external buffer.</p>
	 *
	 * <p>This buffer is designed to reduce copying cost, and is sort of hack...</p>
	 */
	private class WrappedBuffer {
		// This `current` base row points to the head of the buffer.
		private BaseRow current = null;
		// `currentKey` contains the join key of `current`.
		private BinaryRow currentKey = null;

		private Projection<BaseRow, BinaryRow> projection;
		private boolean currentKeyShouldFilter = true;

		// This `cache` base row is the tail of the queue.
		// It is only a reference to the reusable inputs of the operator.
		private BaseRow cache = null;
		// This `externalBuffer` contains the contents of the materialized `cache`s.
		// It's head is the head of the buffer.
		private ResettableExternalBuffer externalBuffer;
		// This `externalIterator` is used to read the contents of `externalBuffer`.
		// Only after all the records in `externalBuffer` are consumed
		// can one read the content of `cache` using the `nextRow()` method.
		private ResettableExternalBuffer.BufferIterator externalIterator;

		private WrappedBuffer(
			List<MemorySegment> memory,
			AbstractRowSerializer serializer,
			Projection<BaseRow, BinaryRow> projection) {
			this.projection = projection;

			this.externalBuffer = new ResettableExternalBuffer(memManager, ioManager, memory, serializer);
			this.externalIterator = this.externalBuffer.newIterator();
		}

		/**
		 * Adds a record to the end of the queue (that is to say, the `cache` base row).
		 *
		 * <p>Note that before adding, the `cache` base row must be empty.
		 * Otherwise the record it contains will be lost.</p>
		 */
		private void add(BaseRow row) {
			Preconditions.checkState(
				cache == null, "Old cache must be materialized to buffer. This is a bug.");
			cache = row;
		}

		/**
		 * Materialize the content of `cache` into `externalBuffer`
		 * (so that the content is preserved after `cache` is reused by the input of the operator).
		 */
		private void materializeCache() throws IOException {
			if (cache != null) {
				externalBuffer.add(cache);
				if (isCacheJustRead()) {
					// This condition means that cache is just read.
					// As cache is read, we should not return its content again when we call `getNext()` method.
					// So the newly materialized `cache` should be skipped.
					externalIterator.advanceNext();
					// The content of `externalIterator.getRow()` is the same with `cache` currently.
					// But we have to do this, becuase the content of `cache` will change after being reused.
					current = externalIterator.getRow();
				}
				cache = null;
			}
		}

		/**
		 * Try to discard all records in the buffer.
		 *
		 * <p>This method is called by the operator
		 * if all the contents in this buffer will no longer be used.</p>
		 */
		private void discard() {
			// As we cannot discard some part of the `externalBuffer` (we can only discard all or none),
			// we require all records in `externalBuffer` to be consumed
			// (that is to say, we've just read the `cache`) before discarding.
			if (isCacheJustRead()) {
				reset();
			}
		}

		private boolean isCacheJustRead() {
			return current == cache;
		}

		private boolean hasNext() {
			return (cache != null && current != cache) || externalIterator.hasNext();
		}

		/**
		 * Get the next unread row of the buffer.
		 *
		 * <p>It will first try to consume the records in `externalBuffer`.
		 * If all records in `externalBuffer` are consumed, it will return the `cache`.</p>
		 *
		 * <p>The content of `currentKey` will also be calculated.</p>
		 */
		private BaseRow nextRow() {
			if (externalIterator.hasNext()) {
				externalIterator.advanceNext();
				current = externalIterator.getRow();
			} else {
				current = cache;
			}

			if (current == null || isEndRow(current)) {
				currentKey = null;
				currentKeyShouldFilter = true;
			} else {
				currentKey = projection.apply(current);
				currentKeyShouldFilter = shouldFilterNull(currentKey);
			}

			return current;
		}

		/**
		 * Reset this buffer to an empty buffer.
		 */
		private void reset() {
			current = null;
			currentKey = null;

			cache = null;
			clearBuffer();
		}

		private void close() {
			externalIterator.close();
			externalBuffer.close();
		}

		/**
		 * Clear all contents in `externalBuffer`, but do nothing to `cache` and `current`.
		 */
		private void clearBuffer() {
			if (externalBuffer.size() > 0) {
				externalIterator.close();
				externalBuffer.reset();
				externalIterator = externalBuffer.newIterator();
			}
		}

		private boolean isBuild() {
			// Due to the property of merge join, one of the two buffers has just read its `cache`.
			// So we select this buffer to be the build buffer.
			//
			// Why don't we use `!hasNext()` instead of `current == cache`?
			// Consider this situation:
			//   1. Record ('A', 1) is received from input 1 ('A' is the key).
			//      As nothing can be done currently, ('A', 1) is materialized to `externalBuffer`.
			//   2. Record ('A', 2) is received from input 2, and is stored in its `cache`.
			//      Both ('A', 1) and ('A', 2) are read, and a merge is going to take place.
			//   3. The `hasNext()` method of both buffers for input 1 and input 2 will return false,
			//      Because their records are read in step 2. If we select buffer 1 as the build buffer,
			//      the content in its `externalBuffer` will be wrongly cleared.
			//      Here we should select buffer 2 as the build buffer,
			//      because the first record to be merged is in its `cache`.
			return current == cache;
		}

		private void enterBuildMode() {
			// All records except the one in `cache` is of no use now.
			// So we clear the content of the `externalBuffer` to store all the build rows.
			// `externalIterator` can be used to iterate through all build rows in the merge stage.
			clearBuffer();
		}

		private void leaveBuildMode() {
			if (externalIterator.hasNext()) {
				// The first "non-build" row is not in `cache`,
				// so we can't clear the external buffer
				// and should move one step forward to the first "non-build" row.
				externalIterator.advanceNext();
			} else {
				// The first "non-build" row is in `cache`, so the records in external buffer are of no use.
				clearBuffer();
			}
		}

		// ==================== metrics ====================

		private long getUsedMemoryInBytes() {
			return externalBuffer.getUsedMemoryInBytes();
		}

		private int getNumSpillFiles() {
			return externalBuffer.getNumSpillFiles();
		}

		private long getSpillInBytes() {
			return externalBuffer.getSpillInBytes();
		}
	}
}
