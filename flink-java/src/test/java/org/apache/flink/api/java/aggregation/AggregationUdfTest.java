package org.apache.flink.api.java.aggregation;

import static java.util.Arrays.asList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.flink.api.java.operators.AggregationUdf;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class AggregationUdfTest { 
		
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void shouldCallAggregationFunctionsAndSetResult() throws Exception {
		// given
		// setup list with 2 input tuples
		Object o1_1 = mock(Object.class);
		Object o1_2 = mock(Object.class);
		Object o2_1 = mock(Object.class);
		Object o2_2 = mock(Object.class);
		List<Tuple2> input = asList(new Tuple2Builder().add(o1_1, o1_2).add(o2_1, o2_2).build());
		
		// setup 2 aggregation functions operating on different tuple fields
		Object result1 = mock(Object.class);
		Object result2 = mock(Object.class);
		AggregationFunction function1 = mock(AggregationFunction.class);
		AggregationFunction function2 = mock(AggregationFunction.class);
		given(function1.getFieldPosition()).willReturn(0);
		given(function1.getAggregate()).willReturn(result1);
		given(function2.getFieldPosition()).willReturn(1);
		given(function2.getAggregate()).willReturn(result2);

		// setup UDF wrapping the 2 functions
		Tuple2<Object, Object> result = new Tuple2<Object, Object>(null, null);
		AggregationFunction[] functions = { function1, function2 };
		AggregationUdf udf = new AggregationUdf(result, functions);
		
		// setup output collector
		Collector<Tuple2<Object, Object>> collector = mock(Collector.class); 
		
		// when
		udf.reduce(input, collector);

		// then
		// verify calls to aggregation function API
		InOrder inOrder1 = Mockito.inOrder(function1);
		inOrder1.verify(function1).initialize();
		inOrder1.verify(function1).aggregate(o1_1);
		inOrder1.verify(function1).aggregate(o2_1);
		inOrder1.verify(function1).getAggregate();

		InOrder inOrder2 = Mockito.inOrder(function2);
		inOrder2.verify(function2).initialize();
		inOrder2.verify(function2).aggregate(o1_2);
		inOrder2.verify(function2).aggregate(o2_2);
		inOrder2.verify(function2).getAggregate();
		
		// verify result was collected
		Tuple2<Object, Object> expected = new Tuple2<Object, Object>(result1, result2);
		verify(collector).collect(expected);
	}
	
}
