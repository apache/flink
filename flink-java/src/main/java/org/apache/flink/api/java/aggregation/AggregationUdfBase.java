package org.apache.flink.api.java.aggregation;

import static java.lang.String.format;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple;

public class AggregationUdfBase<OUT extends Tuple> extends AbstractRichFunction {
	private static final long serialVersionUID = -7383673230236467603L;

	private int arity;
	
	public AggregationUdfBase(int arity) {
		this.arity = arity;
		Validate.inclusiveBetween(1, Tuple.MAX_ARITY, arity, 
				format("The arity of the intermediate tuple must be between {0} and {1}", 1, Tuple.MAX_ARITY));
	}
	
	@SuppressWarnings("unchecked")
	OUT createResultTuple() {
		String resultTupleClassName = "org.apache.flink.api.java.tuple.Tuple" + String.valueOf(arity);
		OUT result = null;
		try {
			result = (OUT) Class.forName(resultTupleClassName).newInstance();
		} catch (InstantiationException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		}
		return result;
	}
	
}
