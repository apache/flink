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
package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class TopKITCase extends MultipleProgramsTestBase {

	public TopKITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void initiate() {
		ExecutionEnvironment.getExecutionEnvironment().setParallelism(5);
	}

	@Test(expected = InvalidProgramException.class)
	public void topKZero() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		FlatMapOperator<Tuple3<Integer, Long, String>, String> ds = getSourceDataSet(env);
		GroupReduceOperator<String, String> reduceOperator = ds.topK(0);
		List<String> result = reduceOperator.collect();
		assertEquals(0, result.size());
	}

	@Test
	public void topKOrderVerification() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		FlatMapOperator<Tuple3<Integer, Long, String>, String> ds = getSourceDataSet(env);

		GroupReduceOperator<String, String> reduceOperator = ds.topK(3);
		List<String> result = reduceOperator.collect();
		containsResultAsText(result, getSourceStrings());
		assertEquals("Luke Skywalker", result.get(0));
		assertEquals("I am fine.", result.get(1));
		assertEquals("Hi", result.get(2));


		reduceOperator = ds.topK(3, false);
		result = reduceOperator.collect();
		containsResultAsText(result, getSourceStrings());
		assertEquals("Comment#1", result.get(0));
		assertEquals("Comment#10", result.get(1));
		assertEquals("Comment#11", result.get(2));
	}

	@Test
	public void topKExceedSourceSize() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		FlatMapOperator<Tuple3<Integer, Long, String>, String> ds = getSourceDataSet(env);
		GroupReduceOperator<String, String> reduceOperator = ds.topK(22);
		List<String> result = reduceOperator.collect();
		containsResultAsText(result, getSourceStrings());
		assertEquals(21, result.size());
		assertEquals("Luke Skywalker", result.get(0));
		assertEquals("I am fine.", result.get(1));
		assertEquals("Hi", result.get(2));
	}

	@Test
	public void topKComplicatedType() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		MapOperator<Tuple3<Integer, Long, String>, String> reduceOperator = ds.topK(3).
			map(new MapFunction<Tuple3<Integer, Long, String>, String>() {
				@Override
				public String map(Tuple3<Integer, Long, String> value) throws Exception {
					return value.f2;
				}
			});
		List<String> result = reduceOperator.collect();
		containsResultAsText(result, getSourceStrings());
		assertEquals(3, result.size());
		assertEquals("Comment#15", result.get(0));
		assertEquals("Comment#14", result.get(1));
		assertEquals("Comment#13", result.get(2));
	}

	private FlatMapOperator<Tuple3<Integer, Long, String>, String> getSourceDataSet(ExecutionEnvironment env) {
		return CollectionDataSets.get3TupleDataSet(env).flatMap(
			new FlatMapFunction<Tuple3<Integer, Long, String>, String>() {
				@Override
				public void flatMap(Tuple3<Integer, Long, String> value, Collector<String> out) throws Exception {
					out.collect(value.f2);
				}
			});
	}

	private String getSourceStrings() {
		return "Hi\n" +
			"Hello\n" +
			"Hello world\n" +
			"Hello world, how are you?\n" +
			"I am fine.\n" +
			"Luke Skywalker\n" +
			"Comment#1\n" +
			"Comment#2\n" +
			"Comment#3\n" +
			"Comment#4\n" +
			"Comment#5\n" +
			"Comment#6\n" +
			"Comment#7\n" +
			"Comment#8\n" +
			"Comment#9\n" +
			"Comment#10\n" +
			"Comment#11\n" +
			"Comment#12\n" +
			"Comment#13\n" +
			"Comment#14\n" +
			"Comment#15\n";
	}
}
