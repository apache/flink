package org.apache.flink.api.java;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.apache.flink.api.java.aggregation.AggregationOperatorFactory;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.junit.Before;
import org.junit.Test;

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
	
}
