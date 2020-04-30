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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.translation.PlanFilterOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Visitor;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test proper automated assignment of the transformation's name, if not set by the user.
 */
@SuppressWarnings("serial")
public class NamesTest implements Serializable {

	@Test
	public void testDefaultName() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> strs = env.fromCollection(Arrays.asList("a", "b"));

		// WARNING: The test will fail if this line is being moved down in the file (the line-number is hard-coded)
		strs.filter(new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(String value) throws Exception {
				return value.equals("a");
			}
		}).output(new DiscardingOutputFormat<String>());
		Plan plan = env.createProgramPlan();
		testForName("Filter at testDefaultName(NamesTest.java:55)", plan);
	}

	@Test
	public void testGivenName() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> strs = env.fromCollection(Arrays.asList("a", "b"));
		strs.filter(new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean filter(String value) throws Exception {
				return value.equals("a");
			}
		}).name("GivenName").output(new DiscardingOutputFormat<String>());
		Plan plan = env.createProgramPlan();
		testForName("GivenName", plan);
	}

	@Test
	public void testJoinWith() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		List<Tuple1<String>> strLi = new ArrayList<Tuple1<String>>();
		strLi.add(new Tuple1<String>("a"));
		strLi.add(new Tuple1<String>("b"));

		DataSet<Tuple1<String>> strs = env.fromCollection(strLi);
		DataSet<Tuple1<String>> strs1 = env.fromCollection(strLi);
		strs.join(strs1).where(0).equalTo(0).with(new FlatJoinFunction<Tuple1<String>, Tuple1<String>, String>() {
			@Override
			public void join(Tuple1<String> first, Tuple1<String> second, Collector<String> out) throws Exception {
				//
			}
		})
				.output(new DiscardingOutputFormat<String>());
		Plan plan = env.createProgramPlan();
		plan.accept(new Visitor<Operator<?>>() {
			@Override
			public boolean preVisit(Operator<?> visitable) {
				if (visitable instanceof InnerJoinOperatorBase) {
					Assert.assertEquals("Join at testJoinWith(NamesTest.java:93)", visitable.getName());
				}
				return true;
			}

			@Override
			public void postVisit(Operator<?> visitable) {}
		});
	}

	private static void testForName(final String expected, Plan plan) {
		plan.accept(new Visitor<Operator<?>>() {
			@Override
			public boolean preVisit(Operator<?> visitable) {
				if (visitable instanceof PlanFilterOperator<?>) {
					// cast is actually not required. Its just a check for the right element
					PlanFilterOperator<?> filterOp = (PlanFilterOperator<?>) visitable;
					Assert.assertEquals(expected, filterOp.getName());
				}
				return true;
			}

			@Override
			public void postVisit(Operator<?> visitable) {
				//
			}
		});
	}
}
