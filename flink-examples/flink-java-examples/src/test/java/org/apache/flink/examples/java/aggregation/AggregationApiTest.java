package org.apache.flink.examples.java.aggregation;

import static java.util.Arrays.asList;
import static org.apache.flink.api.java.aggregation.Aggregations.average;
import static org.apache.flink.api.java.aggregation.Aggregations.count;
import static org.apache.flink.api.java.aggregation.Aggregations.max;
import static org.apache.flink.api.java.aggregation.Aggregations.min;
import static org.apache.flink.api.java.aggregation.Aggregations.sum;
import static org.apache.flink.api.java.aggregation.Aggregations.key;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.builder.Tuple1Builder;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.api.java.tuple.builder.Tuple3Builder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

public class AggregationApiTest {

	private ExecutionEnvironment env;
	
	@Before
	public void setup() {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfNoAggregationIsSpecified() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>().add(1L).build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);

		// when
		input.aggregate();
	}
	
	@Test
	public void shouldCreateTupleElementForEachAggregation() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>()
				.add(1L)
				.add(2L)
				.add(3L)
				.build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);

		// when
		DataSet<Tuple5<Long, Long, Long, Long, Double>> output = 
				input.aggregate(min(0), max(0), count(), sum(0), average(0));

		// then
		assertThat(output, dataSetWithTuple(1L, 3L, 3L, 6L, 2.0));
	}
	
	@Test
	public void shouldComputeMinAndMaxOfStringsAndCountThem() {
		// given
		Tuple1<String>[] tuples = new Tuple1Builder<String>()
				.add("one")
				.add("two")
				.add("three")
				.build();
		DataSet<Tuple1<String>> input = env.fromElements(tuples);

		// when
		DataSet<Tuple3<String, String, Long>> output = 
				input.aggregate(min(0), max(0), count());

		// then
		assertThat(output, dataSetWithTuple("one", "two", 3L));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfSumIsCalledOnString() {
		// given
		DataSet<Tuple1<String>> input = env.fromElements(new Tuple1Builder<String>().add("one").build());

		// when
		input.aggregate(sum(0));
	}
	
	@Test
	public void shouldComputeAggregationsOnDifferentFields() {
		// given
		Tuple2<Long, Long>[] tuples = new Tuple2Builder<Long, Long>()
				.add(11L, 21L)
				.add(12L, 22L)
				.add(13L, 23L)
				.build();
		DataSet<Tuple2<Long, Long>> input = env.fromElements(tuples);

		// when
		DataSet<Tuple7<Long, Long, Long, Long, Long, Long, Long>> output = 
				input.aggregate(min(0), max(0), average(0), min(1), max(1), average(1), count());

		// then
		assertThat(output, dataSetWithTuple(11L, 13L, 12.0, 21L, 23L, 22.0, 3L));
	}
	
	@Test
	public void shouldComputeTupleMaxArityManyAggregations() {
		// given
		Tuple1<Long>[] tuples = new Tuple1Builder<Long>().add(1L).build();
		DataSet<Tuple1<Long>> input = env.fromElements(tuples);
		int num = Tuple.MAX_ARITY ;
		AggregationFunction[] functions = new AggregationFunction[num];
		Arrays.fill(functions, count());

		// when
		DataSet<Tuple> output = input.aggregate(functions);

		// then
		long results[] = new long[num];
		Arrays.fill(results, 1L);
		assertThat(output, dataSetWithTuple(results));
	}
	
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
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldComputeAggregationAfterGrouping() {
		// given
		Tuple2<String, Long>[] tuples = new Tuple2Builder<String, Long>()
				.add("a", 11L)
				.add("a", 12L)
				.add("b", 21L)
				.add("b", 22L)
				.build();
		DataSet<Tuple2<String, Long>> input = env.fromElements(tuples);

		// when
		DataSet<Tuple2<String, Double>> output = 
				input.groupBy(0).aggregate(average(1));

		// then
		assertThat(output, dataSetWithTuples(asList("a", 11.5), asList("b", 21.5)));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldSelectGroupKeys() {
		// given
		Tuple3<String, String, Long>[] tuples = new Tuple3Builder<String, String, Long>()
				.add("a", "A", 11L)
				.add("a", "B", 21L)
				.add("b", "A", 21L)
				.add("b", "B", 22L)
				.build();
		DataSet<Tuple3<String, String, Long>> input = env.fromElements(tuples);

		// when
		DataSet<Tuple2<String, Double>> output = 
				input.groupBy(0, 1).aggregate(key(0), average(2));

		// then
		assertThat(output, dataSetWithTuples(asList("a", 11.5), asList("b", 21.5)));
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorIfKeyIsUsedWithoutGrouping() {
		// given
		Tuple2<String, Long>[] tuples = new Tuple2Builder<String, Long>().add("key", 1L).build();
		DataSet<Tuple2<String, Long>> input = env.fromElements(tuples);

		// when
		input.aggregate(key(0), average(1));
	}
	
	@SuppressWarnings("unchecked")
	private Matcher<DataSet<? extends Tuple>> dataSetWithTuple(Object... singleTuple) {
		return dataSetWithTuples(asList(singleTuple));
	}
	
	private Matcher<DataSet<? extends Tuple>> dataSetWithTuples(final List<? extends Object>... tuples) {
        return new TypeSafeMatcher<DataSet<? extends Tuple>>() {
 
        	List<Tuple> output = null;
 
            @SuppressWarnings({ "unchecked", "rawtypes" }) // TODO can this be made type-safe?
			@Override
            public boolean matchesSafely(DataSet<? extends Tuple> item) {
            	output = new ArrayList<Tuple>();
                item.output(new LocalCollectionOutputFormat(output));
                try {
                    env.execute("TestCase");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if ( output.isEmpty() ) {
                    return false;
                } 
                if ( output.size() != tuples.length ) {
                	return false;
                }
                for (int i = 0; i < tuples.length; ++i) {
                	List<? extends Object> expectedTuple = tuples[i];
                	Tuple actualTuple = output.get(i);
	                if (actualTuple.getArity() != expectedTuple.size()) {
	                    return false;
	                }
	                for (int j = 0; j < expectedTuple.size(); ++j) {
	                    Object actual = actualTuple.getField(j);
	                    Object expected = expectedTuple.get(j);
	                    if (actual != null && ! actual.equals(expected)) {
	                        return false;
	                    }
	                }
                }
                return true;
            }
 
            @Override
            public void describeTo(Description description) {
                description.appendText("a DataSet containing ");
                description.appendText(String.valueOf(tuples.length));
                description.appendText(" tuple(s): ");
                description.appendValueList("", ", ", "", tuples);
            }
 
            @Override
            protected void describeMismatchSafely(
                    DataSet<? extends Tuple> item,
                    Description mismatchDescription) {
                if (output == null) {
                    super.describeMismatchSafely(item, mismatchDescription);
                } else {
                    mismatchDescription
                            .appendText("was a DataSet containing: ");
                    mismatchDescription.appendText(String.valueOf(output.size()));
                    mismatchDescription.appendText(" tuple(s): ");
                    mismatchDescription.appendValueList("", ", ", "", output);
                }
            }
 
        };
    }}
