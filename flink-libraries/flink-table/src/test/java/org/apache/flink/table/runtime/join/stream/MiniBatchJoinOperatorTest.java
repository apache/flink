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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.bundle.CountCoBundleTrigger;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.util.StreamExecUtil;
import org.apache.flink.table.runtime.join.stream.bundle.MiniBatchInnerJoinStreamOperator;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyNotContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.WithoutPrimaryKeyStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertThat;

/**
 * Test reduce logic for miniBatch join.
 */
public class MiniBatchJoinOperatorTest {

	private BaseRowTypeInfo rowType = new BaseRowTypeInfo(
		BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
	private KeySelector<BaseRow, BaseRow> leftKeySelector =
			StreamExecUtil.getKeySelector(new int[]{0}, rowType);
	private KeySelector<BaseRow, BaseRow> rightKeySelector =
			StreamExecUtil.getKeySelector(new int[]{0}, rowType);

	private CountCoBundleTrigger<BaseRow, BaseRow> coBundleTrigger = new CountCoBundleTrigger<>(5);

	@Test
	public void testReduceList() {
		MiniBatchInnerJoinStreamOperator innerjoin = new MiniBatchInnerJoinStreamOperator(null, null,
				null, leftKeySelector, rightKeySelector, null, null, null, null, 0, 0, true, true,
				new boolean[]{false}, coBundleTrigger, true);

		List<Tuple2<BaseRow, Long>> reducedList = new LinkedList<>();
		List<BaseRow> list = new LinkedList<>();
		list.add(hOf((byte) 1, 1));
		list.add(hOf((byte) 1, 1));
		list.add(hOf((byte) 0, 2));
		list.add(hOf((byte) 0, 1));
		list.add(hOf((byte) 1, 2));
		list.add(hOf((byte) 0, 2));
		list.add(hOf((byte) 0, 2));
		list.add(hOf((byte) 1, 3));

		JoinStateHandler joinStateHandler = new WithoutPrimaryKeyStateHandler(null, null);
		reducedList = innerjoin.reduceCurrentList(list, joinStateHandler, true);

		List<Tuple2<BaseRow, Long>> expectedList = new LinkedList<>();
		expectedList.add(Tuple2.of(hOf((byte) 0, 1), -1L));
		expectedList.add(Tuple2.of(hOf((byte) 0, 2), 2L));
		expectedList.add(Tuple2.of(hOf((byte) 0, 3), -1L));
		assertThat(reducedList, org.hamcrest.CoreMatchers.is(expectedList));
	}

	@Test
	public void testUpsertReduceListForKeyValue() {
		MiniBatchInnerJoinStreamOperator innerjoin = new MiniBatchInnerJoinStreamOperator(null, null,
				null, leftKeySelector, rightKeySelector, null, null, null, null, 0, 0, false, false,
				new boolean[]{false}, coBundleTrigger, true);

		List<Tuple2<BaseRow, Long>> reducedList = new LinkedList<>();
		List<BaseRow> list = new LinkedList<>();
		list.add(hOf((byte) 0, 1));
		list.add(hOf((byte) 0, 2));
		list.add(hOf((byte) 0, 3));
		list.add(hOf((byte) 0, 4));
		list.add(hOf((byte) 0, 5));
		list.add(hOf((byte) 0, 6));
		list.add(hOf((byte) 0, 7));
		list.add(hOf((byte) 0, 8));

		JoinStateHandler joinStateHandler = new JoinKeyContainPrimaryKeyStateHandler(null, null);
		reducedList = innerjoin.reduceCurrentList(list, joinStateHandler, false);

		List<Tuple2<BaseRow, Long>> expectedList = new LinkedList<>();
		expectedList.add(Tuple2.of(hOf((byte) 0, 8), 1L));
		assertThat(reducedList, org.hamcrest.CoreMatchers.is(expectedList));
	}

	@Test
	public void testUpsertReduceListForKeyMap() {
		MiniBatchInnerJoinStreamOperator innerjoin = new MiniBatchInnerJoinStreamOperator(null, null,
				null, leftKeySelector, rightKeySelector, null, null, null, null, 0, 0, false, false,
			new boolean[]{false}, coBundleTrigger, true);

		List<Tuple2<BaseRow, Long>> reducedList = new LinkedList<>();
		List<BaseRow> list = new LinkedList<>();
		list.add(hOf((byte) 0, 1, 1));
		list.add(hOf((byte) 0, 3, 1));
		list.add(hOf((byte) 0, 3, 2));
		list.add(hOf((byte) 0, 3, 3));
		list.add(hOf((byte) 0, 2, 1));
		list.add(hOf((byte) 0, 2, 2));
		list.add(hOf((byte) 0, 4, 1));
		list.add(hOf((byte) 0, 4, 2));
		list.add(hOf((byte) 0, 4, 3));
		list.add(hOf((byte) 0, 4, 4));

		Projection<BaseRow, BaseRow> pkProjection = new TestProjection();
		JoinStateHandler joinStateHandler = new JoinKeyNotContainPrimaryKeyStateHandler(null, null, pkProjection);
		reducedList = innerjoin.reduceCurrentList(list, joinStateHandler, false);

		List<Tuple2<BaseRow, Long>> expectedList = new LinkedList<>();
		expectedList.add(Tuple2.of(hOf((byte) 0, 1, 1), 1L));
		expectedList.add(Tuple2.of(hOf((byte) 0, 3, 3), 1L));
		expectedList.add(Tuple2.of(hOf((byte) 0, 2, 2), 1L));
		expectedList.add(Tuple2.of(hOf((byte) 0, 4, 4), 1L));
		assertThat(reducedList, org.hamcrest.CoreMatchers.is(expectedList));
	}

	public GenericRow hOf(byte header, Object... objects) {
		GenericRow row = GenericRow.of(objects);
		row.setHeader(header);
		return row;
	}

	class TestProjection extends Projection<BaseRow, BaseRow> {
		@Override
		public BaseRow apply(BaseRow row) {
			int pk = row.getInt(0);
			return hOf((byte) 0, pk);
		}
	}
}
