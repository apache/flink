package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.apache.flink.util.TestHelper.uniqueString;

import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

public class CompositeAggregationFunctionTest {

	@SuppressWarnings("serial")
	private static class MockCompositeAggregationFunction extends CompositeAggregationFunction<Object, Object> {

		public MockCompositeAggregationFunction(String name, int field) {
			super(name, field);
		}
		
		@Override
		public List<AggregationFunction<?, ?>> getIntermediateAggregationFunctions() {
			return null;
		}

		@Override
		public Object computeComposite(Tuple tuple) {
			return null;
		}

		@Override
		public AggregationFunction.ResultTypeBehavior getResultTypeBehavior() {
			return null;
		}

		@Override
		public BasicTypeInfo<Object> getResultType() {
			return null;
		}

		@Override
		public void setInputType(BasicTypeInfo<Object> inputType) {

		}

	}
	
	private CompositeAggregationFunction<Object, Object> composite;
	
	@Before
	public void setup() {
		int field = uniqueInt();
		String name = uniqueString();
		composite = new MockCompositeAggregationFunction(name, field);
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldNotCallInitialize() {
		// when
		composite.initialize(null);
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldNotCallReduce() {
		// when
		composite.reduce(null, null);
	}
	
}
