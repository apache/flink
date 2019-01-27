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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.util.Iterator;

/**
 * Inner join operator based on hash.
 */
@Internal
public class InnerJoinStreamOperator extends JoinStreamOperator {

	private static final long serialVersionUID = 1L;

	public InnerJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType, long maxRetentionTime, long minRetentionTime,
			boolean[] filterNullKeys) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinStateType, rightJoinStateType, maxRetentionTime, minRetentionTime,
				filterNullKeys);
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("leftJoinStateType {}, rightJoinStateType {}", leftJoinStateType, rightJoinStateType);
		LOG.info("Init InnerJoinStreamOperator.");
	}

	// call processElement1 if input is from the left of join
	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		processElement(
			getOrCopyBaseRow(element, true),
			leftStateHandler,
			rightStateHandler,
			true,
			leftTimerState,
			stateCleaningEnabled);

		return TwoInputSelection.ANY;
	}


	// call processElement2 if input is from the right of join
	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		processElement(
			getOrCopyBaseRow(element, false),
			rightStateHandler,
			leftStateHandler,
			false,
			rightTimerState,
			stateCleaningEnabled);

		return TwoInputSelection.ANY;
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		byte namespace = timer.getNamespace();
		if (namespace == 1) {
			//left
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), leftTimerState)) {
				leftStateHandler.remove(timer.getKey());
			}

		} else {
			//right
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), rightTimerState)) {
				rightStateHandler.remove(timer.getKey());
			}
		}
	}

	private void processElement(
			BaseRow input,
			JoinStateHandler inputStateHandler,
			JoinStateHandler otherStateHandler,
			boolean inputIsLeft, KeyedValueState<BaseRow, Long> timerState,
			boolean cleaningBasedTimer) throws Exception {
		inputStateHandler.extractCurrentJoinKey(input);
		inputStateHandler.extractCurrentPrimaryKey(input);

		long currentTime = internalTimerService.currentProcessingTime();

		if (BaseRowUtil.isRetractMsg(input)) {
			//retract
			inputStateHandler.retract(input);
		} else {
			//register timer
			registerProcessingCleanupTimer(inputStateHandler.getCurrentJoinKey(), currentTime, inputIsLeft,
					timerState);

			inputStateHandler.add(input, currentTime + maxRetentionTime);
		}

		Iterator<Tuple3<BaseRow, Long, Long>> iterator = otherStateHandler.getRecords(
				inputStateHandler.getCurrentJoinKey());
		joinedRow.setHeader(input.getHeader());
		while (iterator.hasNext()) {
			Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
			BaseRow baseRow = tuple3.f0;
			long count = tuple3.f1;
			if (inputIsLeft) {
				if (applyCondition(input, baseRow,
					inputStateHandler.getCurrentJoinKey())) {
					joinedRow.replace(input, baseRow);
					for (int i = 0; i < count; i++) {
						collector.collect(joinedRow);
					}
				}
			} else {
				if (applyCondition(baseRow, input,
					inputStateHandler.getCurrentJoinKey())) {
					for (int i = 0; i < count; i++) {
						joinedRow.replace(baseRow, input);
						collector.collect(joinedRow);
					}
				}
			}
			if (tuple3.f2 <= currentTime && stateCleaningEnabled) {
				//expire
				iterator.remove();
			}
		}
	}
}
