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

package org.apache.flink.client.program;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.fail;

/**
 * Tests that verify subsequent calls to {@link ExecutionEnvironment#getExecutionPlan()} and
 * {@link ExecutionEnvironment#execute()}/{@link ExecutionEnvironment#createProgramPlan()} do not cause any exceptions.
 */
@SuppressWarnings("serial")
public class ExecutionPlanAfterExecutionTest extends TestLogger implements Serializable {

	@Test
	public void testExecuteAfterGetExecutionPlan() {
		ExecutionEnvironment env = new LocalEnvironment();

		DataSet<Integer> baseSet = env.fromElements(1, 2);

		DataSet<Integer> result = baseSet.map(new MapFunction<Integer, Integer>() {
			@Override public Integer map(Integer value) throws Exception {
				return value * 2;
			}});
		result.output(new DiscardingOutputFormat<Integer>());

		try {
			env.getExecutionPlan();
			env.execute();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cannot run both #getExecutionPlan and #execute.");
		}
	}

	@Test
	public void testCreatePlanAfterGetExecutionPlan() {
		ExecutionEnvironment env = new LocalEnvironment();

		DataSet<Integer> baseSet = env.fromElements(1, 2);

		DataSet<Integer> result = baseSet.map(new MapFunction<Integer, Integer>() {
			@Override public Integer map(Integer value) throws Exception {
				return value * 2;
			}});
		result.output(new DiscardingOutputFormat<Integer>());

		try {
			env.getExecutionPlan();
			env.createProgramPlan();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Cannot run both #getExecutionPlan and #execute. Message: " + e.getMessage());
		}
	}

	@Test
	public void testGetExecutionPlanOfRangePartition() {
		ExecutionEnvironment env = new LocalEnvironment();

		DataSet<Integer> baseSet = env.fromElements(1, 2);

		DataSet<Tuple2<Integer, Integer>> result = baseSet
			.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Integer value) throws Exception {
					return new Tuple2(value, value * 2);
				}
			})
			.partitionByRange(0)
			.aggregate(Aggregations.MAX, 1);
		result.output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

		try {
			env.getExecutionPlan();
			env.execute();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cannot run both #getExecutionPlan and #execute.");
		}
	}
}
