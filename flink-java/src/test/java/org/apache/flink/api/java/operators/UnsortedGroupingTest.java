package org.apache.flink.api.java.operators;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationOperatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.junit.Before;
import org.junit.Test;

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
		DataSet<Tuple2<String, Long>> input = env.fromElements(new Tuple2Builder<String, Long>()
				.add("a", 1L)
				.add("a", 2L)
				.build());
		UnsortedGrouping<Tuple2<String, Long>> grouping = input.groupBy(0);
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

}
