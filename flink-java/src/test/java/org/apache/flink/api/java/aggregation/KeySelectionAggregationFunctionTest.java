package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class KeySelectionAggregationFunctionTest {

	private KeySelectionAggregationFunction<Integer> key = new KeySelectionAggregationFunction<Integer>(0);
	
	@Test
	public void shouldReturnFirst() {
		// given
		int value1 = uniqueInt();
		int value2 = uniqueInt(new int[] {value1});

		// when
		key.initialize();
		key.aggregate(value1);
		key.aggregate(value2);
		Integer actual = key.getAggregate();

		// then
		assertThat(actual, is(value1));
	}
	
	@Test
	public void shouldReset() {
		// given
		int value1 = uniqueInt();

		// when
		key.aggregate(value1);
		key.initialize();
		Integer actual = key.getAggregate();

		// then
		assertThat(actual, is(nullValue()));
	}
	
}
