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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for duplicate elimination in the solution set.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class SolutionSetDuplicatesITCase extends MultipleProgramsTestBase {

	public SolutionSetDuplicatesITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testProgram() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> data = env
				.generateSequence(0, 10)
				.flatMap(new FlatMapFunction<Long, Tuple2<Long, Long>>() {
					@Override
					public void flatMap(Long value, Collector<Tuple2<Long, Long>> out) {
						out.collect(new Tuple2<Long, Long>(value, value));
						out.collect(new Tuple2<Long, Long>(value, value));
						out.collect(new Tuple2<Long, Long>(value, value));
					}
				})
				.rebalance();

			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter = data.iterateDelta(data, 10, 0);

			List<Integer> result = iter
					.closeWith(iter.getWorkset(), iter.getWorkset())
					.map(new MapFunction<Tuple2<Long, Long>, Integer>() {
						@Override
						public Integer map(Tuple2<Long, Long> value) {
							return value.f0.intValue();
						}
					})
					.collect();

			assertEquals(11, result.size());

			Collections.sort(result);
			assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), result);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
