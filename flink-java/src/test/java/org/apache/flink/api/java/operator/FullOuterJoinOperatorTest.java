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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DataSet#fullOuterJoin(DataSet)}.
 */
public class FullOuterJoinOperatorTest {

	// TUPLE DATA
	private static final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
		new ArrayList<>();

	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new
		TupleTypeInfo<>(
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.LONG_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.LONG_TYPE_INFO,
		BasicTypeInfo.INT_TYPE_INFO
	);

	@Test
	public void testFullOuter1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
			.where(0).equalTo(4)
			.with(new DummyJoin());
	}

	@Test
	public void testFullOuter2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
				.where("f1").equalTo("f3")
				.with(new DummyJoin());
	}

	@Test
	public void testFullOuter3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
				.where(new IntKeySelector()).equalTo(new IntKeySelector())
				.with(new DummyJoin());
	}

	@Test
	public void testFullOuter4() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
				.where(0).equalTo(new IntKeySelector())
				.with(new DummyJoin());
	}

	@Test
	public void testFullOuter5() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
				.where(new IntKeySelector()).equalTo("f4")
				.with(new DummyJoin());
	}

	@Test
	public void testFullOuter6() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2)
				.where("f0").equalTo(4)
				.with(new DummyJoin());
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testFullOuter7() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// invalid key position
		ds1.fullOuterJoin(ds2)
				.where(5).equalTo(0)
				.with(new DummyJoin());
	}

	@Test(expected = CompositeType.InvalidFieldReferenceException.class)
	public void testFullOuter8() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// invalid key reference
		ds1.fullOuterJoin(ds2)
				.where(1).equalTo("f5")
				.with(new DummyJoin());
	}

	@Test(expected = InvalidProgramException.class)
	public void testFullOuter9() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// key types do not match
		ds1.fullOuterJoin(ds2)
				.where(0).equalTo(1)
				.with(new DummyJoin());
	}

	@Test(expected = InvalidProgramException.class)
	public void testFullOuter10() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// key types do not match
		ds1.fullOuterJoin(ds2)
				.where(new IntKeySelector()).equalTo(new LongKeySelector())
				.with(new DummyJoin());
	}

	@Test
	public void testFullOuterStrategy1() {
		this.testFullOuterStrategies(JoinHint.OPTIMIZER_CHOOSES);
	}

	@Test
	public void testFullOuterStrategy2() {
		this.testFullOuterStrategies(JoinHint.REPARTITION_SORT_MERGE);
	}

	@Test
	public void testFullOuterStrategy3() {
		this.testFullOuterStrategies(JoinHint.REPARTITION_HASH_SECOND);
	}

	@Test(expected = InvalidProgramException.class)
	public void testFullOuterStrategy4() {
		this.testFullOuterStrategies(JoinHint.BROADCAST_HASH_SECOND);
	}

	@Test
	public void testFullOuterStrategy5() {
		this.testFullOuterStrategies(JoinHint.REPARTITION_HASH_FIRST);
	}

	@Test(expected = InvalidProgramException.class)
	public void testFullOuterStrategy6() {
		this.testFullOuterStrategies(JoinHint.BROADCAST_HASH_FIRST);
	}

	private void testFullOuterStrategies(JoinHint hint) {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.fullOuterJoin(ds2, hint)
				.where(0).equalTo(4)
				.with(new DummyJoin());
	}

	/*
	 * ####################################################################
	 */

	@SuppressWarnings("serial")
	private static class DummyJoin implements
			JoinFunction<Tuple5<Integer, Long, String, Long, Integer>, Tuple5<Integer, Long, String, Long, Integer>, Long> {

		@Override
		public Long join(Tuple5<Integer, Long, String, Long, Integer> v1, Tuple5<Integer, Long, String, Long, Integer> v2) throws Exception {
			return 1L;
		}
	}

	@SuppressWarnings("serial")
	private static class IntKeySelector implements KeySelector<Tuple5<Integer, Long, String, Long, Integer>, Integer> {

		@Override
		public Integer getKey(Tuple5<Integer, Long, String, Long, Integer> v) throws Exception {
			return v.f0;
		}
	}

	@SuppressWarnings("serial")
	private static class LongKeySelector implements KeySelector<Tuple5<Integer, Long, String, Long, Integer>, Long> {

		@Override
		public Long getKey(Tuple5<Integer, Long, String, Long, Integer> v) throws Exception {
			return v.f1;
		}
	}

}
