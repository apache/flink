/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

package org.apache.flink.streaming.api.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.datastream.StreamProjection;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Test;

public class ProjectTest implements Serializable {
	private static final long serialVersionUID = 1L;

	@Test
	public void operatorTest() {

		TypeInformation<Tuple5<Integer, String, Integer, String, Integer>> inType = TypeExtractor
				.getForObject(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "b", 4));

		int[] fields = new int[]{4, 4, 3};

		TupleSerializer<Tuple3<Integer, Integer, String>> serializer =
				new TupleTypeInfo<Tuple3<Integer, Integer, String>>(StreamProjection.extractFieldTypes(fields, inType))
						.createSerializer(new ExecutionConfig());
		@SuppressWarnings("unchecked")
		StreamProject<Tuple5<Integer, String, Integer, String, Integer>, Tuple3<Integer, Integer, String>> operator =
				new StreamProject<Tuple5<Integer, String, Integer, String, Integer>, Tuple3<Integer, Integer, String>>(
						fields, serializer);

		List<Tuple5<Integer, String, Integer, String, Integer>> input = new ArrayList<Tuple5<Integer, String, Integer,
				String, Integer>>();
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "b", 4));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "s", 3, "c", 2));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "c", 2));
		input.add(new Tuple5<Integer, String, Integer, String, Integer>(2, "a", 3, "a", 7));

		List<Tuple3<Integer, Integer, String>> expected = new ArrayList<Tuple3<Integer, Integer, String>>();
		expected.add(new Tuple3<Integer, Integer, String>(4, 4, "b"));
		expected.add(new Tuple3<Integer, Integer, String>(2, 2, "c"));
		expected.add(new Tuple3<Integer, Integer, String>(2, 2, "c"));
		expected.add(new Tuple3<Integer, Integer, String>(7, 7, "a"));

		assertEquals(expected, MockContext.createAndExecute(operator, input));
	}


	// tests using projection from the API without explicitly specifying the types
	private static final long MEMORY_SIZE = 32;
	private static HashSet<Tuple2<Long, Double>> expected = new HashSet<Tuple2<Long, Double>>();
	private static HashSet<Tuple2<Long, Double>> actual = new HashSet<Tuple2<Long, Double>>();

	@Test
	public void APIWithoutTypesTest() {

		for (Long i = 1L; i < 11L; i++) {
			expected.add(new Tuple2<Long, Double>(i, i.doubleValue()));
		}

		StreamExecutionEnvironment env = new TestStreamEnvironment(1, MEMORY_SIZE);

		env.generateParallelSequence(1, 10).map(new MapFunction<Long, Tuple3<Long, Character, Double>>() {
				@Override
				public Tuple3<Long, Character, Double> map(Long value) throws Exception {
					return new Tuple3<Long, Character, Double>(value, 'c', value.doubleValue());
				}
			})
			.project(0, 2)
			.addSink(new SinkFunction<Tuple>() {
				@Override
				@SuppressWarnings("unchecked")
				public void invoke(Tuple value) throws Exception {
					actual.add( (Tuple2<Long,Double>) value);
				}
			});

		try {
			env.execute();
		} catch (Exception e) {
			fail(e.getMessage());
		}

		assertEquals(expected, actual);
	}
}
