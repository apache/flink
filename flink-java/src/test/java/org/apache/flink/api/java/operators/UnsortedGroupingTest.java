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

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationBuilder;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationOperatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(UnsortedGrouping.class)
public class UnsortedGroupingTest {

	private ExecutionEnvironment env;
	
	@Before
	public void setup() {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void shouldDelegateAggregate() {
		// given
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationOperatorFactory aggregationOperatorFactory = mock(AggregationOperatorFactory.class);
		AggregationOperator expected = mock(AggregationOperator.class);
		AggregationFunction[] functions = { mock(AggregationFunction.class) };
		given(aggregationOperatorFactory.aggregate(grouping, functions)).willReturn(expected);
		grouping.setAggregationOperatorFactory(aggregationOperatorFactory);

		// when
		AggregationOperator actual = grouping.aggregate(functions);
		
		// then
		assertThat(actual, is(expected));
	}

	@Test
	public void shouldHaveSingletonAggregationFactory() {
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();

		// when
		AggregationOperatorFactory aggregationOperatorFactory = grouping.getAggregationOperatorFactory();
		
		// then
		assertThat(aggregationOperatorFactory, is(not(nullValue())));
		assertThat(aggregationOperatorFactory, is(AggregationOperatorFactory.getInstance()));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithMin() throws Exception {
		// given
		int field = uniqueInt();
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.min(field);

		// then
		assertThat(actual, is(builder));
		verify(builder).min(field);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithMax() throws Exception {
		// given
		int field = uniqueInt();
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.max(field);

		// then
		assertThat(actual, is(builder));
		verify(builder).max(field);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithSum() throws Exception {
		// given
		int field = uniqueInt();
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.sum(field);

		// then
		assertThat(actual, is(builder));
		verify(builder).sum(field);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithCount() throws Exception {
		// given
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.count();

		// then
		assertThat(actual, is(builder));
		verify(builder).count();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithAverage() throws Exception {
		// given
		int field = uniqueInt();
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.average(field);

		// then
		assertThat(actual, is(builder));
		verify(builder).average(field);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldStartAggregationWithKey() throws Exception {
		// given
		int field = uniqueInt();
		UnsortedGrouping<Tuple2<String, Long>> grouping = createGrouping();
		AggregationBuilder builder = mock(AggregationBuilder.class);
		whenNew(AggregationBuilder.class).withArguments(grouping).thenReturn(builder);

		// when
		AggregationBuilder actual = grouping.key(field);

		// then
		assertThat(actual, is(builder));
		verify(builder).key(field);
	}

	private UnsortedGrouping<Tuple2<String, Long>> createGrouping() {
		DataSet<Tuple2<String, Long>> input = env.fromElements(new Tuple2Builder<String, Long>()
				.add("a", 1L)
				.add("a", 2L)
				.build());
		UnsortedGrouping<Tuple2<String, Long>> grouping = input.groupBy(0);
		return grouping;
	}
	
}
