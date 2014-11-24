package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Test;

public class AggregationUdfBaseTest {

	private AggregationUdfBase<Tuple> udf;
	
	@Test
	public void shouldCreateResultTuple() {
		// given
		int arity = uniqueInt(1, Tuple.MAX_ARITY + 1);
		udf = new AggregationUdfBase<Tuple>(arity);

		// when
		Tuple result = udf.createResultTuple();

		// then
		assertThat(result.getArity(), is(arity));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfResultTupleTooLarge() {
		// given
		int arity = uniqueInt(Tuple.MAX_ARITY + 1, Tuple.MAX_ARITY + 10);
		
		// when
		udf = new AggregationUdfBase<Tuple>(arity);
	}

}
