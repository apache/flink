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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/**
 * Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN.
 */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

	private static final long serialVersionUID = -376944622236540545L;

	// whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
	private final boolean leftIsOuter;
	// whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
	private final boolean rightIsOuter;

	private transient JoinedRowData outRow;
	private transient RowData leftNullRow;
	private transient RowData rightNullRow;

	// left join state
	private transient JoinRecordStateView leftRecordStateView;
	// right join state
	private transient JoinRecordStateView rightRecordStateView;

	public StreamingJoinOperator(
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean leftIsOuter,
			boolean rightIsOuter,
			boolean[] filterNullKeys,
			long minRetentionTime) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec, rightInputSideSpec, filterNullKeys, minRetentionTime);
		this.leftIsOuter = leftIsOuter;
		this.rightIsOuter = rightIsOuter;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.outRow = new JoinedRowData();
		this.leftNullRow = new GenericRowData(leftType.toRowSize());
		this.rightNullRow = new GenericRowData(rightType.toRowSize());

		// initialize states
		if (leftIsOuter) {
			this.leftRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				minRetentionTime);
		} else {
			this.leftRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				minRetentionTime);
		}

		if (rightIsOuter) {
			this.rightRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				minRetentionTime);
		} else {
			this.rightRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				minRetentionTime);
		}
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
	}

	/**
	 * Process an input element and output incremental joined records, retraction messages will
	 * be sent in some scenarios.
	 *
	 * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
	 * method is too complex, so we provide the pseudo code to help understand the logic. We should
	 * keep sync the following pseudo code with the real logic of the method.
	 *
	 * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
	 * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise,
	 * we always send insert and delete for simplification. We can optimize this to send -U & +U
	 * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
	 * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now
	 * for simplification. See {@code FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
	 *
	 * <pre>
	 * if input record is accumulate
	 * |  if input side is outer
	 * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
	 * |  |  if there are matched rows on the other side
	 * |  |  | if other side is outer
	 * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
	 * |  |  | |  if the matched num in the matched rows > 0, skip
	 * |  |  | |  otherState.update(other, old + 1)
	 * |  |  | endif
	 * |  |  | send +I[record+other]s, state.add(record, other.size)
	 * |  |  endif
	 * |  endif
	 * |  if input side not outer
	 * |  |  state.add(record)
	 * |  |  if there is no matched rows on the other side, skip
	 * |  |  if there are matched rows on the other side
	 * |  |  |  if other side is outer
	 * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
	 * |  |  |  |  if the matched num in the matched rows > 0, skip
	 * |  |  |  |  otherState.update(other, old + 1)
	 * |  |  |  |  send +I[record+other]s
	 * |  |  |  else
	 * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
	 * |  |  |  endif
	 * |  |  endif
	 * |  endif
	 * endif
	 *
	 * if input record is retract
	 * |  state.retract(record)
	 * |  if there is no matched rows on the other side
	 * |  | if input side is outer, send -D[record+null]
	 * |  endif
	 * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
	 * |  |  if other side is outer
	 * |  |  |  if the matched num in the matched rows == 0, this should never happen!
	 * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
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
			RowData input,
			JoinRecordStateView inputSideStateView,
			JoinRecordStateView otherSideStateView,
			boolean inputIsLeft) throws Exception {
		boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
		boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
		boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
		RowKind inputRowKind = input.getRowKind();
		input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

		AssociatedRecords associatedRecords = AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
		if (isAccumulateMsg) { // record is accumulate
			if (inputIsOuter) { // input side is outer
				OuterJoinRecordStateView inputSideOuterStateView = (OuterJoinRecordStateView) inputSideStateView;
				if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
					// send +I[record+null]
					outRow.setRowKind(RowKind.INSERT);
					outputNullPadding(input, inputIsLeft);
					// state.add(record, 0)
					inputSideOuterStateView.addRecord(input, 0);
				} else { // there are matched rows on the other side
					if (otherIsOuter) { // other side is outer
						OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
						for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
							RowData other = outerRecord.record;
							// if the matched num in the matched rows == 0
							if (outerRecord.numOfAssociations == 0) {
								// send -D[null+other]
								outRow.setRowKind(RowKind.DELETE);
								outputNullPadding(other, !inputIsLeft);
							} // ignore matched number > 0
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(other, outerRecord.numOfAssociations + 1);
						}
					}
					// send +I[record+other]s
					outRow.setRowKind(RowKind.INSERT);
					for (RowData other : associatedRecords.getRecords()) {
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
								// send -D[null+other]
								outRow.setRowKind(RowKind.DELETE);
								outputNullPadding(outerRecord.record, !inputIsLeft);
							}
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(outerRecord.record, outerRecord.numOfAssociations + 1);
						}
						// send +I[record+other]s
						outRow.setRowKind(RowKind.INSERT);
					} else {
						// send +I/+U[record+other]s (using input RowKind)
						outRow.setRowKind(inputRowKind);
					}
					for (RowData other : associatedRecords.getRecords()) {
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
					// send -D[record+null]
					outRow.setRowKind(RowKind.DELETE);
					outputNullPadding(input, inputIsLeft);
				}
				// nothing to do when input side is not outer
			} else { // there are matched rows on the other side
				if (inputIsOuter) {
					// send -D[record+other]s
					outRow.setRowKind(RowKind.DELETE);
				} else {
					// send -D/-U[record+other]s (using input RowKind)
					outRow.setRowKind(inputRowKind);
				}
				for (RowData other : associatedRecords.getRecords()) {
					output(input, other, inputIsLeft);
				}
				// if other side is outer
				if (otherIsOuter) {
					OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
					for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
						if (outerRecord.numOfAssociations == 1) {
							// send +I[null+other]
							outRow.setRowKind(RowKind.INSERT);
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

	private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
		if (inputIsLeft) {
			outRow.replace(inputRow, otherRow);
		} else {
			outRow.replace(otherRow, inputRow);
		}
		collector.collect(outRow);
	}

	private void outputNullPadding(RowData row, boolean isLeft) {
		if (isLeft) {
			outRow.replace(row, rightNullRow);
		} else {
			outRow.replace(leftNullRow, row);
		}
		collector.collect(outRow);
	}
}
