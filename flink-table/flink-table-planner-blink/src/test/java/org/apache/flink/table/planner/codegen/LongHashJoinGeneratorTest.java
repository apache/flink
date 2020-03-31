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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTest;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link LongHashJoinGenerator}.
 */
public class LongHashJoinGeneratorTest extends Int2HashJoinOperatorTest {

	@Override
	public Object newOperator(long memorySize, HashJoinType type, boolean reverseJoinFunction) {
		RowType keyType = RowType.of(new IntType());
		Assert.assertTrue(LongHashJoinGenerator.support(type, keyType, new boolean[] {true}));
		return LongHashJoinGenerator.gen(
				new TableConfig(), type,
				keyType,
				RowType.of(new IntType(), new IntType()),
				RowType.of(new IntType(), new IntType()),
				new int[]{0},
				new int[]{0},
				memorySize, memorySize, 0, 20, 10000,
				reverseJoinFunction,
				new GeneratedJoinCondition(MyJoinCondition.class.getCanonicalName(), "", new Object[0])
		);
	}

	@Test
	@Override
	public void testBuildLeftSemiJoin() throws Exception {}

	@Test
	@Override
	public void testBuildSecondHashFullOutJoin() throws Exception {}

	@Test
	@Override
	public void testBuildSecondHashRightOutJoin() throws Exception {}

	@Test
	@Override
	public void testBuildLeftAntiJoin() throws Exception {}

	@Test
	@Override
	public void testBuildFirstHashLeftOutJoin() throws Exception {}

	@Test
	@Override
	public void testBuildFirstHashFullOutJoin() throws Exception {}

	/**
	 * Test cond.
	 */
	public static class MyJoinCondition extends AbstractRichFunction implements JoinCondition {

		public MyJoinCondition(Object[] reference) {}

		@Override
		public boolean apply(BaseRow in1, BaseRow in2) {
			return true;
		}
	}
}
