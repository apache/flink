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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.NonBatchOnlyEqualityConditionMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.util.Iterator;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;

/**
 * Base operator for outer join based on hash.
 */
@Internal
abstract class OuterJoinStreamOperator extends JoinStreamOperator {

	private static final long serialVersionUID = 1L;

	protected final JoinMatchStateHandler.Type leftMatchStateType;
	protected final JoinMatchStateHandler.Type rightMatchStateType;

	protected transient JoinMatchStateHandler leftMatchStateHandler;
	protected transient JoinMatchStateHandler rightMatchStateHandler;

	protected transient BaseRow leftSideNullRow;
	protected transient BaseRow rightSideNullRow;

	public OuterJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType, long maxRetentionTime, long minRetentionTime,
			JoinMatchStateHandler.Type leftMatchStateType,
			JoinMatchStateHandler.Type rightMatchStateType, boolean[] filterNullKeys) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinStateType, rightJoinStateType, maxRetentionTime, minRetentionTime,
				filterNullKeys);
		this.leftMatchStateType = leftMatchStateType;
		this.rightMatchStateType = rightMatchStateType;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.leftSideNullRow = new GenericRow(leftType.getArity());
		this.rightSideNullRow = new GenericRow(rightType.getArity());
		LOG.info("leftJoinStateType {}, rightJoinStateType {}, leftMatchStateType {}, rightMatchStateType {}",
				leftJoinStateType, rightJoinStateType, leftMatchStateType, rightMatchStateType);
	}

	@Override
	protected void initAllStates() throws Exception {
		super.initAllStates();

		this.leftMatchStateHandler = createMatchStateHandler(
			leftType, leftMatchStateType, leftKeyType, "LeftMatchHandler", leftPkProjectCode);
		this.rightMatchStateHandler = createMatchStateHandler(
			rightType, rightMatchStateType, rightKeyType, "RightMatchHandler", rightPkProjectCode);
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		byte namespace = timer.getNamespace();
		if (namespace == 1) {
			//left
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), leftTimerState)) {
				leftMatchStateHandler.remove(timer.getKey());
				leftStateHandler.remove(timer.getKey());
			}
		} else {
			//right
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), rightTimerState)) {
				rightMatchStateHandler.remove(timer.getKey());
				rightStateHandler.remove(timer.getKey());
			}
		}
	}

	protected TwoInputSelection processElement(BaseRow input, JoinStateHandler inputSideStateHandler,
			JoinStateHandler otherSideStateHandler, JoinMatchStateHandler inputSideMatchStateHandler,
			JoinMatchStateHandler otherSideMatchStateHandler,
			boolean inputIsLeft, boolean inputIsOuter, boolean otherSideIsOuter,
			KeyedValueState<BaseRow, Long> timerState) throws Exception {
		long currentTime = internalTimerService.currentProcessingTime();
		inputSideStateHandler.extractCurrentJoinKey(input);
		inputSideStateHandler.extractCurrentPrimaryKey(input);

		long possibleJoinCnt = getOtherSidePossibleMatchJoinCnt(otherSideMatchStateHandler, inputSideStateHandler,
				inputSideStateHandler.getCurrentJoinKey());

		long inputCount;
		if (BaseRowUtil.isRetractMsg(input)) {
			//retract
			inputCount = inputSideStateHandler.retract(input);
		} else {
			//register timer
			registerProcessingCleanupTimer(inputSideStateHandler.getCurrentJoinKey(), currentTime, inputIsLeft,
					timerState);

			//update
			inputCount = inputSideStateHandler.add(input, currentTime + maxRetentionTime);
		}

		byte reservedHeader = input.getHeader();
		joinedRow.setHeader(reservedHeader);

		Iterator<Tuple3<BaseRow, Long, Long>> iterator = otherSideStateHandler.getRecords(
				inputSideStateHandler.getCurrentJoinKey());

		long inputJoinOtherSideRowNum = 0;

		while (iterator.hasNext()) {
			Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
			BaseRow matchRow = tuple3.f0;
			long matchRowCount = tuple3.f1;
			boolean isMatched = false;
			if (inputIsLeft) {
				if (applyCondition(input, matchRow,
					inputSideStateHandler.getCurrentJoinKey())) {
					isMatched = true;
				}
			} else {
				if (applyCondition(matchRow, input,
					inputSideStateHandler.getCurrentJoinKey())) {
					isMatched = true;
				}
			}
			if (isMatched) {
				inputJoinOtherSideRowNum += matchRowCount;
				collectJoinResult(input, inputSideStateHandler, matchRow, matchRowCount,
						inputIsLeft, otherSideIsOuter, otherSideMatchStateHandler, possibleJoinCnt);
			}
			if (tuple3.f2 < currentTime && stateCleaningEnabled) {
				//expire
				iterator.remove();
				otherSideMatchStateHandler.remove(inputSideStateHandler.getCurrentJoinKey(), matchRow);
			}
		}

		if (inputJoinOtherSideRowNum == 0 && inputIsOuter) {
			//not match
			if (inputIsLeft) {
				collectResult(joinedRow.replace(input, rightSideNullRow));
			} else {
				collectResult(joinedRow.replace(leftSideNullRow, input));
			}
		}

		if (inputCount <= 0) {
			//remove
			input.setHeader(ACCUMULATE_MSG);
			inputSideMatchStateHandler.remove(inputSideStateHandler.getCurrentJoinKey(), input);
			input.setHeader(reservedHeader);
		} else if (inputCount == 1 && BaseRowUtil.isAccumulateMsg(input)) {
			//input appear firstly
			inputSideMatchStateHandler.updateRowMatchJoinCnt(
					inputSideStateHandler.getCurrentJoinKey(), input, inputJoinOtherSideRowNum);
		}
		return TwoInputSelection.ANY;
	}

	private void collectJoinResult(
			BaseRow input, JoinStateHandler inputSideStateHandler, BaseRow matchedRow, long matchedRowCount, boolean
			inputIsLeft, boolean matchSideIsOuter, JoinMatchStateHandler matchSideMatchStateHandler,
			long possibleJoinCnt) {
		byte reservedHeader = input.getHeader();
		BaseRow currentJoinKey = inputSideStateHandler.getCurrentJoinKey();

		matchSideMatchStateHandler.extractCurrentRowMatchJoinCount(currentJoinKey, matchedRow, possibleJoinCnt);

		if (matchSideIsOuter) {

			//produce retraction here.
			if (matchSideMatchStateHandler.getCurrentRowMatchJoinCnt() == 0 &&
					BaseRowUtil.isAccumulateMsg(input)) {
				//retract join result with null
				joinedRow.setHeader(RETRACT_MSG);
				for (int i = 0; i < matchedRowCount; i++) {
					if (inputIsLeft) {
						collectResult(joinedRow.replace(leftSideNullRow, matchedRow));
					} else {
						collectResult(joinedRow.replace(matchedRow, rightSideNullRow));
					}
				}
				joinedRow.setHeader(reservedHeader);
			}
		}

		long appendJoinCnt = BaseRowUtil.isRetractMsg(input) ? -1L : 1L;
		long updateJoinCnt = matchSideMatchStateHandler.getCurrentRowMatchJoinCnt() + appendJoinCnt;
		matchSideMatchStateHandler.resetCurrentRowMatchJoinCnt(updateJoinCnt);

		for (int i = 0; i < matchedRowCount; i++) {
			if (inputIsLeft) {
				collectResult(joinedRow.replace(input, matchedRow));
			} else {
				collectResult(joinedRow.replace(matchedRow, input));
			}
		}

		if (matchSideIsOuter) {
			if (matchSideMatchStateHandler.getCurrentRowMatchJoinCnt() == 0 && BaseRowUtil.isRetractMsg(input)) {
				//send join result with null
				joinedRow.setHeader(ACCUMULATE_MSG);
				for (int i = 0; i < matchedRowCount; i++) {
					if (inputIsLeft) {
						collectResult(joinedRow.replace(leftSideNullRow, matchedRow));
					} else {
						collectResult(joinedRow.replace(matchedRow, rightSideNullRow));
					}
				}
				joinedRow.setHeader(reservedHeader);
			}
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

	private void collectResult(BaseRow row) {
		collector.collect(row);
	}
}
