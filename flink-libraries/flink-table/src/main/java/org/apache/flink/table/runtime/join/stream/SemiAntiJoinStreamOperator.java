/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.NonBatchOnlyEqualityConditionMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.util.Iterator;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;
import static org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler.Type.EMPTY_MATCH;
import static org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler.Type.ONLY_EQUALITY_CONDITION_EMPTY_MATCH;

/**
 * Semi&Anti join operator.
 */
public class SemiAntiJoinStreamOperator extends JoinStreamOperator {

	private boolean isAntiJoin;

	private final JoinMatchStateHandler.Type leftMatchStateType;

	//record all match times as far as possible if the operator cares the matchState.
	private final boolean careLeftMatchTimes;

	protected final JoinStateHandler.Type leftEmitedJoinStateType;

	protected final JoinStateHandler.Type leftNotEmitedJoinStateType;

	private transient JoinStateHandler leftEmitedStateHandler;

	private transient JoinStateHandler leftNotEmitedStateHandler;

	private transient JoinMatchStateHandler leftMatchStateHandler;

	private boolean rightNotEmitRetraction;

	public SemiAntiJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType, long maxRetentionTime, long minRetentionTime, boolean isAntiJoin,
			JoinMatchStateHandler.Type leftMatchStateType,
			boolean rightNotEmitRetraction, boolean[] filterNullKeys) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinStateType, rightJoinStateType, maxRetentionTime, minRetentionTime, filterNullKeys);
		this.isAntiJoin = isAntiJoin;
		this.rightNotEmitRetraction = rightNotEmitRetraction;

		if (isAntiJoin) {
			if (rightNotEmitRetraction) {
				this.leftNotEmitedJoinStateType = JoinStateHandler.Type.EMPTY;
			} else {
				this.leftNotEmitedJoinStateType = leftJoinStateType;
			}
			this.leftEmitedJoinStateType = leftJoinStateType;
		} else {
			if (rightNotEmitRetraction) {
				this.leftEmitedJoinStateType = JoinStateHandler.Type.EMPTY;
			} else {
				this.leftEmitedJoinStateType = leftJoinStateType;
			}
			this.leftNotEmitedJoinStateType = leftJoinStateType;
		}

		this.leftMatchStateType = leftMatchStateType;

		if (leftMatchStateType.equals(EMPTY_MATCH) || leftMatchStateType.equals(ONLY_EQUALITY_CONDITION_EMPTY_MATCH)) {
			this.careLeftMatchTimes = false;
		} else {
			this.careLeftMatchTimes = true;
		}
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("leftNotEmitedJoinStateType {}, leftEmitedJoinStateType {}, leftMatchStateType {}, " +
				"rightJoinStateType {}, rightNotEmitRetraction {}", this.leftNotEmitedJoinStateType,
				this.leftEmitedJoinStateType, this.leftMatchStateType, rightNotEmitRetraction);
		LOG.info("Init SemiAntiJoinStreamOperator.");
	}

	@Override
	protected void initAllStates() throws Exception {
		this.leftEmitedStateHandler = createJoinStateHandler(leftType, leftEmitedJoinStateType, "leftEmitedState",
				leftKeySelector, leftKeyType, leftPkProjectCode);

		this.leftNotEmitedStateHandler = createJoinStateHandler(leftType, leftNotEmitedJoinStateType,
				"leftNotEmitedState",
				leftKeySelector, leftKeyType, leftPkProjectCode);

		this.leftMatchStateHandler = createMatchStateHandler(
				leftType, leftMatchStateType, leftKeyType, "LeftMatchHandler", leftPkProjectCode);

		this.rightStateHandler = createJoinStateHandler(rightType, rightJoinStateType, "rightJoinState",
				rightKeySelector, rightKeyType, rightPkProjectCode);

		if (stateCleaningEnabled) {
			this.leftTimerState = createCleanupTimeState("left-time-state");
			this.rightTimerState = createCleanupTimeState("right-time-state");
		}
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		long currentTime = internalTimerService.currentProcessingTime();
		BaseRow input = getOrCopyBaseRow(element, true);
		BaseRow joinKey = leftKeySelector.getKey(input);

		if (BaseRowUtil.isAccumulateMsg(input)) {
			//register timer
			registerProcessingCleanupTimer(joinKey, currentTime, true, leftTimerState);
		}

		boolean canEmit = isAntiJoin ? true : false;
		if (leftEmitedStateHandler.contains(joinKey, input)) {
			canEmit = true;
		} else {
			Iterator<Tuple3<BaseRow, Long, Long>> iterator = rightStateHandler.getRecords(joinKey);
			long inputRowMatchOtherSideNum = 0;
			while (iterator.hasNext()) {
				Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
				BaseRow matchRow = tuple3.f0;
				if (tuple3.f2 < currentTime && stateCleaningEnabled) {
					//expire
					iterator.remove();
				} else {
					if (applyCondition(input, matchRow,
						leftKeySelector.getKey(input))) {
						inputRowMatchOtherSideNum += tuple3.f1;
						canEmit = isAntiJoin ? false : true;
						if (!careLeftMatchTimes) {
							break;
						}
					}
				}
			}
			if (careLeftMatchTimes && inputRowMatchOtherSideNum > 0 && BaseRowUtil.isAccumulateMsg(input)) {
				leftMatchStateHandler.updateRowMatchJoinCnt(joinKey, input, inputRowMatchOtherSideNum);
			}
		}

		if (canEmit) {
			collector.collect(input);
			leftEmitedStateHandler.extractCurrentJoinKey(input);
			leftEmitedStateHandler.extractCurrentPrimaryKey(input);
			if (BaseRowUtil.isRetractMsg(input)) {
				leftEmitedStateHandler.retract(input);
			} else {
				leftEmitedStateHandler.add(input, currentTime + maxRetentionTime);
			}
		} else {
			leftNotEmitedStateHandler.extractCurrentJoinKey(input);
			leftNotEmitedStateHandler.extractCurrentPrimaryKey(input);
			if (BaseRowUtil.isRetractMsg(input)) {
				leftNotEmitedStateHandler.retract(input);
			} else {
				leftNotEmitedStateHandler.add(input, currentTime + maxRetentionTime);
			}
		}
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = getOrCopyBaseRow(element, false);
		if (isAntiJoin) {
			processReceivedRightRow(input, leftNotEmitedStateHandler, leftEmitedStateHandler);
		} else {
			processReceivedRightRow(input, leftEmitedStateHandler, leftNotEmitedStateHandler);
		}
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {

	}

	@Override
	public void endInput2() throws Exception {

	}

	private void processReceivedRightRow(BaseRow rightRow, JoinStateHandler leftJoinStateHandler,
			JoinStateHandler leftNotJoinStateHandler) throws Exception {
		long currentTime = internalTimerService.currentProcessingTime();
		rightStateHandler.extractCurrentJoinKey(rightRow);
		rightStateHandler.extractCurrentPrimaryKey(rightRow);
		BaseRow joinKey = rightStateHandler.getCurrentJoinKey();

		if (BaseRowUtil.isRetractMsg(rightRow)) {
			long possibleJoinCnt = getOtherSidePossibleMatchJoinCnt(leftMatchStateHandler, rightStateHandler, joinKey);
			Iterator<Tuple3<BaseRow, Long, Long>> iterator = leftJoinStateHandler.getRecords(joinKey);
			while (iterator.hasNext()) {
				Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
				BaseRow matchedRow = tuple3.f0;
				if (tuple3.f2 < currentTime && stateCleaningEnabled) {
					//expire
					iterator.remove();
					leftMatchStateHandler.remove(joinKey, matchedRow);
				} else {
					if (applyCondition(matchedRow, rightRow,
						rightStateHandler.getCurrentJoinKey())) {
						leftMatchStateHandler.extractCurrentRowMatchJoinCount(joinKey, matchedRow, possibleJoinCnt);
						long currentJoinCnt = leftMatchStateHandler.getCurrentRowMatchJoinCnt() - 1;
						if (currentJoinCnt == 0) {
							leftMatchStateHandler.remove(joinKey, matchedRow);
							if (!isAntiJoin) {
								matchedRow.setHeader(RETRACT_MSG);
							}
							for (int i = 0; i < tuple3.f1; i++) {
								collector.collect(matchedRow);
							}
							matchedRow.setHeader(ACCUMULATE_MSG);
							leftNotJoinStateHandler.update(joinKey, matchedRow, tuple3.f1, tuple3.f2);
							iterator.remove();
						} else {
							leftMatchStateHandler.resetCurrentRowMatchJoinCnt(currentJoinCnt);
						}
					}
				}
			}
			rightStateHandler.retract(rightRow);
		} else {
			//register timer
			registerProcessingCleanupTimer(joinKey, currentTime, false, rightTimerState);

			Iterator<Tuple3<BaseRow, Long, Long>> iterator = leftJoinStateHandler.getRecords(joinKey);
			while (iterator.hasNext()) {
				Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
				BaseRow matchedRow = tuple3.f0;
				if (tuple3.f2 < currentTime && stateCleaningEnabled) {
					//expire
					iterator.remove();
					leftMatchStateHandler.remove(joinKey, matchedRow);
				} else {
					if (applyCondition(matchedRow, rightRow,
						rightStateHandler.getCurrentJoinKey())) {
						leftMatchStateHandler.addRowMatchJoinCnt(joinKey, matchedRow, 1);
					}
				}
			}

			Iterator<Tuple3<BaseRow, Long, Long>> iterator1 = leftNotJoinStateHandler.getRecords(joinKey);
			while (iterator1.hasNext()) {
				Tuple3<BaseRow, Long, Long> tuple3 = iterator1.next();
				BaseRow matchedRow = tuple3.f0;
				if (tuple3.f2 < currentTime && stateCleaningEnabled) {
					//expire
					iterator.remove();
					leftMatchStateHandler.remove(joinKey, matchedRow);
				} else {
					if (applyCondition(matchedRow, rightRow,
						rightStateHandler.getCurrentJoinKey())) {
						if (isAntiJoin) {
							matchedRow.setHeader(RETRACT_MSG);
						}
						for (int i = 0; i < tuple3.f1; i++) {
							collector.collect(matchedRow);
						}
						matchedRow.setHeader(ACCUMULATE_MSG);
						leftMatchStateHandler.updateRowMatchJoinCnt(joinKey, matchedRow, 1);
						leftJoinStateHandler.update(joinKey, matchedRow, tuple3.f1, tuple3.f2);
						iterator1.remove();
					}
				}
			}
			rightStateHandler.add(rightRow, currentTime + maxRetentionTime);
		}
	}

	private long getOtherSidePossibleMatchJoinCnt(JoinMatchStateHandler otherSideStateHandler, JoinStateHandler
			inputSideStateHandler, BaseRow joinKey) {
		if (otherSideStateHandler instanceof NonBatchOnlyEqualityConditionMatchStateHandler) {
			//calculate the matchJoinCount based the otherSideStateHandler.
			Iterator<Tuple3<BaseRow, Long, Long>> iterator = inputSideStateHandler.getRecords(joinKey);
			long matchJoinCount = 0;
			while (iterator.hasNext() && (matchJoinCount < 2)) {
				Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
				matchJoinCount += tuple3.f1;
			}
			return matchJoinCount;
		} else {
			//other handlers don't care the possible join count.
			return 0L;
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		byte namespace = timer.getNamespace();
		if (namespace == 1) {
			//left
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), leftTimerState)) {
				leftEmitedStateHandler.remove(timer.getKey());
				leftNotEmitedStateHandler.remove(timer.getKey());
				leftMatchStateHandler.remove(timer.getKey());
			}
		} else {
			//right
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), rightTimerState)) {
				rightStateHandler.remove(timer.getKey());
			}
		}
	}
}
