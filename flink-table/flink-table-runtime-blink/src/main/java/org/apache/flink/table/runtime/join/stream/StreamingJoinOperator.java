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

package org.apache.flink.table.runtime.join.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.generated.GeneratedJoinCondition;
import org.apache.flink.table.generated.JoinCondition;
import org.apache.flink.table.runtime.join.NullAwareJoinHelper;
import org.apache.flink.table.runtime.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN.
 */
public class StreamingJoinOperator extends AbstractStreamOperator<BaseRow>
	implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = -376944622236540545L;

	private final BaseRowTypeInfo leftType;
	private final BaseRowTypeInfo rightType;
	private final GeneratedJoinCondition generatedJoinCondition;

	private final JoinInputSideSpec leftInputSideSpec;
	private final JoinInputSideSpec rightInputSideSpec;

	// whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
	private final boolean leftIsOuter;
	// whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
	private final boolean rightIsOuter;

	/**
	 * Should filter null keys.
	 */
	private final int[] nullFilterKeys;

	/**
	 * No keys need to filter null.
	 */
	private final boolean nullSafe;

	/**
	 * Filter null to all keys.
	 */
	private final boolean filterAllNulls;

	private final long minRetentionTime;
	private final boolean stateCleaningEnabled;

	private transient JoinCondition joinCondition;
	private transient TimestampedCollector<BaseRow> collector;

	private transient JoinedRow outRow;
	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;

	// left join state
	private transient JoinRecordStateView leftRecordStateView;
	// right join state
	private transient JoinRecordStateView rightRecordStateView;

	public StreamingJoinOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean leftIsOuter,
			boolean rightIsOuter,
			boolean[] filterNullKeys,
			long minRetentionTime) {
		this.leftType = leftType;
		this.rightType = rightType;
		this.generatedJoinCondition = generatedJoinCondition;
		this.leftInputSideSpec = leftInputSideSpec;
		this.rightInputSideSpec = rightInputSideSpec;
		this.minRetentionTime = minRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
		this.leftIsOuter = leftIsOuter;
		this.rightIsOuter = rightIsOuter;
		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNullKeys);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNullKeys.length;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.joinCondition = new JoinConditionWithNullFilters(
			generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader()));

		this.collector = new TimestampedCollector<>(output);
		this.outRow = new JoinedRow();
		this.leftNullRow = new GenericRow(leftType.getArity());
		this.rightNullRow = new GenericRow(rightType.getArity());

		// initialize states
		if (leftIsOuter) {
			this.leftRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				minRetentionTime,
				stateCleaningEnabled);
		} else {
			this.leftRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				minRetentionTime,
				stateCleaningEnabled);
		}

		if (rightIsOuter) {
			this.rightRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				minRetentionTime,
				stateCleaningEnabled);
		} else {
			this.rightRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				minRetentionTime,
				stateCleaningEnabled);
		}
	}

	@Override
	public void processElement1(StreamRecord<BaseRow> element) throws Exception {
		processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
	}

	@Override
	public void processElement2(StreamRecord<BaseRow> element) throws Exception {
		processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
	}

	/**
	 * Process an input element and output incremental joined records, retraction messages will
	 * be sent in some scenarios.
	 *
	 * <p>Following is the pseudocode to describe the core logic of this method. The logic of this
	 * method is too complex, so we provide the pseudocode to help understand the logic. We should
	 * keep sync the following pseudocode with the real logic of the method.
	 *
	 * <pre>
	 * if input record is accumulate
	 * |  if input side is outer
	 * |  |  if there is no matched rows on the other side, send +[record+null], state.add(record, 0)
	 * |  |  if there are matched rows on the other side
	 * |  |  | if other side is outer
	 * |  |  | |  if the matched num in the matched rows == 0, send -[null+other]
	 * |  |  | |  if the matched num in the matched rows > 0, skip
	 * |  |  | |  otherState.update(other, old + 1)
	 * |  |  | endif
	 * |  |  | send +[record+other]s, state.add(record, other.size)
	 * |  |  endif
	 * |  endif
	 * |  if input side not outer
	 * |  |  state.add(record)
	 * |  |  if there is no matched rows on the other side, skip
	 * |  |  if there are matched rows on the other side
	 * |  |  |  if other side is outer
	 * |  |  |  |  if the matched num in the matched rows == 0, send -[null+other]
	 * |  |  |  |  if the matched num in the matched rows > 0, skip
	 * |  |  |  |  otherState.update(other, old + 1)
	 * |  |  |  endif
	 * |  |  |  send +[record+other]s
	 * |  |  endif
	 * |  endif
	 * endif
	 *
	 * if input record is retract
	 * |  state.retract(record)
	 * |  if there is no matched rows on the other side
	 * |  | if input side is outer, send -[record+null]
	 * |  endif
	 * |  if there are matched rows on the other side, send -[record+other]s
	 * |  |  if other side is outer
	 * |  |  |  if the matched num in the matched rows == 0, this should never happen!
	 * |  |  |  if the matched num in the matched rows == 1, send +[null+other]
	 * |  |  |  if the matched num in the matched rows > 1, skip
	 * |  |  |  otherState.update(other, old - 1)
	 * |  |  endif
	 * |  endif
	 * endif
	 * </pre>
	 *
	 * @param input the input element
	 * @param inputSideStateView state of input side
	 * @param otherSideStateView state of other side
	 * @param inputIsLeft whether input side is left side
	 */
	private void processElement(
			BaseRow input,
			JoinRecordStateView inputSideStateView,
			JoinRecordStateView otherSideStateView,
			boolean inputIsLeft) throws Exception {
		boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
		boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;

		AssociatedRecords associatedRecords = AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
		if (BaseRowUtil.isAccumulateMsg(input)) { // record is accumulate
			if (inputIsOuter) { // input side is outer
				OuterJoinRecordStateView inputSideOuterStateView = (OuterJoinRecordStateView) inputSideStateView;
				if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
					// send +[record+null]
					outRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
					outputNullPadding(input, inputIsLeft);
					// state.add(record, 0)
					inputSideOuterStateView.addRecord(input, 0);
				} else { // there are matched rows on the other side
					if (otherIsOuter) { // other side is outer
						OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
						for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
							BaseRow other = outerRecord.record;
							// if the matched num in the matched rows == 0
							if (outerRecord.numOfAssociations == 0) {
								// send -[null+other]
								outRow.setHeader(BaseRowUtil.RETRACT_MSG);
								outputNullPadding(other, !inputIsLeft);
							} // ignore matched number > 0
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(other, outerRecord.numOfAssociations + 1);
						}
					}
					// send +[record+other]s
					outRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
					for (BaseRow other : associatedRecords.getRecords()) {
						output(input, other, inputIsLeft);
					}
					// state.add(record, other.size)
					inputSideOuterStateView.addRecord(input, associatedRecords.size());
				}
			} else { // input side not outer
				// state.add(record)
				inputSideStateView.addRecord(input);
				if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side
					if (otherIsOuter) { // if other side is outer
						OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
						for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
							if (outerRecord.numOfAssociations == 0) { // if the matched num in the matched rows == 0
								// send -[null+other]
								outRow.setHeader(BaseRowUtil.RETRACT_MSG);
								outputNullPadding(outerRecord.record, !inputIsLeft);
							}
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(outerRecord.record, outerRecord.numOfAssociations + 1);
						}
					}
					// send +[record+other]s
					outRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
					for (BaseRow other : associatedRecords.getRecords()) {
						output(input, other, inputIsLeft);
					}
				}
				// skip when there is no matched rows on the other side
			}
		} else { // input record is retract
			// state.retract(record)
			inputSideStateView.retractRecord(input);
			if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
				if (inputIsOuter) { // input side is outer
					// send -[record+null]
					outRow.setHeader(BaseRowUtil.RETRACT_MSG);
					outputNullPadding(input, inputIsLeft);
				}
				// nothing to do when input side is not outer
			} else { // there are matched rows on the other side
				// send -[record+other]s
				outRow.setHeader(BaseRowUtil.RETRACT_MSG);
				for (BaseRow other : associatedRecords.getRecords()) {
					output(input, other, inputIsLeft);
				}
				// if other side is outer
				if (otherIsOuter) {
					OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
					for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
						if (outerRecord.numOfAssociations == 1) {
							// send +[null+other]
							outRow.setHeader(BaseRowUtil.ACCUMULATE_MSG);
							outputNullPadding(outerRecord.record, !inputIsLeft);
						} // nothing else to do when number of associations > 1
						// otherState.update(other, old - 1)
						otherSideOuterStateView.updateNumOfAssociations(outerRecord.record, outerRecord.numOfAssociations - 1);
					}
				}
			}
		}
	}

	// -------------------------------------------------------------------------------------

	private void output(BaseRow inputRow, BaseRow otherRow, boolean inputIsLeft) {
		if (inputIsLeft) {
			outRow.replace(inputRow, otherRow);
		} else {
			outRow.replace(otherRow, inputRow);
		}
		collector.collect(outRow);
	}

	private void outputNullPadding(BaseRow row, boolean isLeft) {
		if (isLeft) {
			outRow.replace(row, rightNullRow);
		} else {
			outRow.replace(leftNullRow, row);
		}
		collector.collect(outRow);
	}

	// ----------------------------------------------------------------------------------------
	// Utility Classes
	// ----------------------------------------------------------------------------------------

	private class JoinConditionWithNullFilters implements JoinCondition {

		final JoinCondition backingJoinCondition;

		private JoinConditionWithNullFilters(JoinCondition backingJoinCondition) {
			this.backingJoinCondition = backingJoinCondition;
		}

		@Override
		public boolean apply(BaseRow left, BaseRow right) {
			if (!nullSafe) { // is not null safe, return false if any null exists
				// key is always BinaryRow
				BinaryRow joinKey = (BinaryRow) getCurrentKey();
				if (filterAllNulls ? joinKey.anyNull() : joinKey.anyNull(nullFilterKeys)) {
					// find null present, return false directly
					return false;
				}
			}
			// test condition
			return backingJoinCondition.apply(left, right);
		}
	}

	/**
	 * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper
	 * of {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and
	 * {@link #getOuterRecords()}. See the method Javadoc for more details.
	 */
	private static final class AssociatedRecords {
		private final List<OuterRecord> records;

		private AssociatedRecords(List<OuterRecord> records) {
			checkNotNull(records);
			this.records = records;
		}

		public boolean isEmpty() {
			return records.isEmpty();
		}

		public int size() {
			return records.size();
		}

		/**
		 * Gets the iterable of records. This is usually be called when the
		 * {@link AssociatedRecords} is from inner side.
		 */
		public Iterable<BaseRow> getRecords() {
			return new RecordsIterable(records);
		}

		/**
		 * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations.
		 * This is usually be called when the {@link AssociatedRecords} is from outer side.
		 */
		public Iterable<OuterRecord> getOuterRecords() {
			return records;
		}

		/**
		 * Creates an {@link AssociatedRecords} which represents the records associated to the
		 * input row.
		 */
		public static AssociatedRecords of(
				BaseRow input,
				boolean inputIsLeft,
				JoinRecordStateView otherSideStateView,
				JoinCondition condition) throws Exception {
			List<OuterRecord> associations = new ArrayList<>();
			if (otherSideStateView instanceof OuterJoinRecordStateView) {
				OuterJoinRecordStateView outerStateView = (OuterJoinRecordStateView) otherSideStateView;
				Iterable<Tuple2<BaseRow, Integer>> records = outerStateView.getRecordsAndNumOfAssociations();
				for (Tuple2<BaseRow, Integer> record : records) {
					boolean matched = inputIsLeft ? condition.apply(input, record.f0) : condition.apply(record.f0, input);
					if (matched) {
						associations.add(new OuterRecord(record.f0, record.f1));
					}
				}
			} else {
				Iterable<BaseRow> records = otherSideStateView.getRecords();
				for (BaseRow record : records) {
					boolean matched = inputIsLeft ? condition.apply(input, record) : condition.apply(record, input);
					if (matched) {
						// use -1 as the default number of associations
						associations.add(new OuterRecord(record, -1));
					}
				}
			}
			return new AssociatedRecords(associations);
		}

	}

	/**
	 * A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<BaseRow>}.
	 */
	private static final class RecordsIterable implements IterableIterator<BaseRow> {
		private final List<OuterRecord> records;
		private int index = 0;

		private RecordsIterable(List<OuterRecord> records) {
			this.records = records;
		}

		@Override
		public Iterator<BaseRow> iterator() {
			index = 0;
			return this;
		}

		@Override
		public boolean hasNext() {
			return index < records.size();
		}

		@Override
		public BaseRow next() {
			BaseRow row = records.get(index).record;
			index++;
			return row;
		}
	}

	/**
	 * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The
	 * {@code numOfAssociations} represents the number of associated records in the other side.
	 * It is used when the record is from outer side (e.g. left side in LEFT OUTER JOIN).
	 * When the {@code numOfAssociations} is ZERO, we need to send a null padding row.
	 * This is useful to avoid recompute the associated numbers every time.
	 *
	 * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the
	 * {@code numOfAssociations} will always be {@code -1}.
	 */
	private static final class OuterRecord {
		private final BaseRow record;
		private final int numOfAssociations;

		private OuterRecord(BaseRow record, int numOfAssociations) {
			this.record = record;
			this.numOfAssociations = numOfAssociations;
		}
	}

}
