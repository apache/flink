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

package org.apache.flink.api.java;

import static org.apache.flink.api.java.aggregation.Aggregations.average;
import static org.apache.flink.api.java.aggregation.Aggregations.count;
import static org.apache.flink.api.java.aggregation.Aggregations.key;
import static org.apache.flink.api.java.aggregation.Aggregations.sum;
import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.Arrays;

import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationOperatorFactory;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple1Builder;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataSet.class)
public class DataSetTest {

	private ExecutionEnvironment env;
	
	@Before
	public void setup() {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void shouldDelegateAggregate() {
		// given
		DataSet input = env.fromElements(uniqueInt());
		AggregationOperatorFactory aggregationOperatorFactory = mock(AggregationOperatorFactory.class);
		AggregationOperator expected = mock(AggregationOperator.class);
		AggregationFunction[] functions = { mock(AggregationFunction.class) };
		given(aggregationOperatorFactory.aggregate(input, functions)).willReturn(expected);
		input.setAggregationOperatorFactory(aggregationOperatorFactory);

		// when
		AggregationOperator actual = input.aggregate(functions);
		
		// then
		assertThat(actual, is(expected));
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void shouldHaveSingletonAggregationFactory() {
		// given
		DataSet input = env.fromElements(uniqueInt());

		// when
		AggregationOperatorFactory aggregationOperatorFactory = input.getAggregationOperatorFactory();
		
		// then
		assertThat(aggregationOperatorFactory, is(not(nullValue())));
		assertThat(aggregationOperatorFactory, is(AggregationOperatorFactory.getInstance()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfNoAggregationIsSpecified() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>().add(1L).build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);

		// when
		input.aggregate();
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfNotATuple() {
		// given
		DataSet<Long> input = env.fromElements(1L);

		// when
		input.aggregate(sum(0));
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfTupleContentIsNotBasicType() {
		// given
		DataSet<Tuple1<Object>> input = env.fromElements(new Tuple1Builder<Object>().add(new Object()).build());

		// when
		input.aggregate(sum(0));
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfNotExistingFieldIsSpecified() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>()
				.add(1L)
				.add(2L)
				.add(3L)
				.build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);

		// when
		input.aggregate(sum(1));
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfSumIsCalledOnString() {
		// given
		DataSet<Tuple1<String>> input = env.fromElements(new Tuple1Builder<String>().add("one").build());

		// when
		input.aggregate(sum(0));
	}

	@SuppressWarnings("rawtypes")
	@Test(expected=IllegalArgumentException.class)
	public void errorIfTooManyAggregations() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>().add(1L).build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);
		int num = Tuple.MAX_ARITY + 1;
		AggregationFunction[] functions = new AggregationFunction[num];
		Arrays.fill(functions, count());

		// when
		input.aggregate(functions);
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfKeyIsUsedWithoutGrouping() {
		// given
		Tuple2<String, Long>[] tuples = new Tuple2Builder<String, Long>().add("key", 1L).build();
		DataSet<Tuple2<String, Long>> input = env.fromElements(tuples);

		// when
		input.aggregate(key(0), average(1));
	}

}
