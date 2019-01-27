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
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

/**
 * Left outer join operator.
 */
public class LeftOuterJoinStreamOperator extends OuterJoinStreamOperator {

	public LeftOuterJoinStreamOperator(
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
				leftMatchStateType, rightMatchStateType, filterNullKeys);
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("Init LeftOuterJoinStreamOperator.");
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		processElement(
			getOrCopyBaseRow(element, true),
			leftStateHandler,
			rightStateHandler,
			leftMatchStateHandler,
			rightMatchStateHandler,
			true,
			true,
			false,
			leftTimerState);
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		processElement(
			getOrCopyBaseRow(element, false),
			rightStateHandler,
			leftStateHandler,
			rightMatchStateHandler,
			leftMatchStateHandler,
			false,
			false,
			true,
			rightTimerState);
		return TwoInputSelection.ANY;
	}
}
