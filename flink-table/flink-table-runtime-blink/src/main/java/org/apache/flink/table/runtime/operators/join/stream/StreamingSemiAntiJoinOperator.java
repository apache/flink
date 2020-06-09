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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.RowKind;

/**
 * Streaming unbounded Join operator which supports SEMI/ANTI JOIN.
 */
public class StreamingSemiAntiJoinOperator extends AbstractStreamingJoinOperator {

	private static final long serialVersionUID = -3135772379944924519L;

	// true if it is anti join, otherwise is semi joinp
	private final boolean isAntiJoin;

	// left join state
	private transient OuterJoinRecordStateView leftRecordStateView;
	// right join state
	private transient JoinRecordStateView rightRecordStateView;

	public StreamingSemiAntiJoinOperator(
			boolean isAntiJoin,
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean[] filterNullKeys,
			long minRetentionTime) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec, rightInputSideSpec, filterNullKeys, minRetentionTime);
		this.isAntiJoin = isAntiJoin;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.leftRecordStateView = OuterJoinRecordStateViews.create(
			getRuntimeContext(),
			LEFT_RECORDS_STATE_NAME,
			leftInputSideSpec,
			leftType,
			minRetentionTime);

		this.rightRecordStateView = JoinRecordStateViews.create(
			getRuntimeContext(),
			RIGHT_RECORDS_STATE_NAME,
			rightInputSideSpec,
			rightType,
			minRetentionTime);
	}

	/**
	 * Process an input element and output incremental joined records, retraction messages will
	 * be sent in some scenarios.
	 *
	 * <p>Following is the pseudo code to describe the core logic of this method.
	 *
	 * <pre>
	 * if there is no matched rows on the other side
	 *   if anti join, send input record
	 * if there are matched rows on the other side
	 *   if semi join, send input record
	 * if the input record is accumulate, state.add(record, matched size)
	 * if the input record is retract, state.retract(record)
	 * </pre>
	 */
	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		RowData input = element.getValue();
		AssociatedRecords associatedRecords = AssociatedRecords.of(input, true, rightRecordStateView, joinCondition);
		if (associatedRecords.isEmpty()) {
			if (isAntiJoin) {
				collector.collect(input);
			}
		} else { // there are matched rows on the other side
			if (!isAntiJoin) {
				collector.collect(input);
			}
		}
		if (RowDataUtil.isAccumulateMsg(input)) {
			// erase RowKind for state updating
			input.setRowKind(RowKind.INSERT);
			leftRecordStateView.addRecord(input, associatedRecords.size());
		} else { // input is retract
			// erase RowKind for state updating
			input.setRowKind(RowKind.INSERT);
			leftRecordStateView.retractRecord(input);
		}
	}

	/**
	 * Process an input element and output incremental joined records, retraction messages will
	 * be sent in some scenarios.
	 *
	 * <p>Following is the pseudo code to describe the core logic of this method.
	 *
	 * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
	 * "-U" represents "UPDATE_BEFORE".
	 *
	 * <pre>
	 * if input record is accumulate
	 * | state.add(record)
	 * | if there is no matched rows on the other side, skip
	 * | if there are matched rows on the other side
	 * | | if the matched num in the matched rows == 0
	 * | |   if anti join, send -D[other]s
	 * | |   if semi join, send +I/+U[other]s (using input RowKind)
	 * | | if the matched num in the matched rows > 0, skip
	 * | | otherState.update(other, old+1)
	 * | endif
	 * endif
	 * if input record is retract
	 * | state.retract(record)
	 * | if there is no matched rows on the other side, skip
	 * | if there are matched rows on the other side
	 * | | if the matched num in the matched rows == 0, this should never happen!
	 * | | if the matched num in the matched rows == 1
	 * | |   if semi join, send -D/-U[other] (using input RowKind)
	 * | |   if anti join, send +I[other]
	 * | | if the matched num in the matched rows > 1, skip
	 * | | otherState.update(other, old-1)
	 * | endif
	 * endif
	 * </pre>
	 */
	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		RowData input = element.getValue();
		boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
		RowKind inputRowKind = input.getRowKind();
		input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

		AssociatedRecords associatedRecords = AssociatedRecords.of(input, false, leftRecordStateView, joinCondition);
		if (isAccumulateMsg) { // record is accumulate
			rightRecordStateView.addRecord(input);
			if (!associatedRecords.isEmpty()) {
				// there are matched rows on the other side
				for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
					RowData other = outerRecord.record;
					if (outerRecord.numOfAssociations == 0) {
						if (isAntiJoin) {
							// send -D[other]
							other.setRowKind(RowKind.DELETE);
						} else {
							// send +I/+U[other] (using input RowKind)
							other.setRowKind(inputRowKind);
						}
						collector.collect(other);
						// set header back to INSERT, because we will update the other row to state
						other.setRowKind(RowKind.INSERT);
					} // ignore when number > 0
					leftRecordStateView.updateNumOfAssociations(other, outerRecord.numOfAssociations + 1);
				}
			} // ignore when associated number == 0
		} else { // retract input
			rightRecordStateView.retractRecord(input);
			if (!associatedRecords.isEmpty()) {
				// there are matched rows on the other side
				for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
					RowData other = outerRecord.record;
					if (outerRecord.numOfAssociations == 1) {
						if (!isAntiJoin) {
							// send -D/-U[other] (using input RowKind)
							other.setRowKind(inputRowKind);
						} else {
							// send +I[other]
							other.setRowKind(RowKind.INSERT);
						}
						collector.collect(other);
						// set RowKind back, because we will update the other row to state
						other.setRowKind(RowKind.INSERT);
					} // ignore when number > 0
					leftRecordStateView.updateNumOfAssociations(other, outerRecord.numOfAssociations - 1);
				}
			} // ignore when associated number == 0
		}
	}
}
