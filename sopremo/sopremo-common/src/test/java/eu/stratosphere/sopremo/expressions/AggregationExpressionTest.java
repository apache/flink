package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import junit.framework.Assert;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.aggregation.BuiltinFunctions;

public class AggregationExpressionTest extends EvaluableExpressionTest<AggregationExpression> {
	@Override
	protected AggregationExpression createDefaultInstance(int index) {
		return new AggregationExpression(BuiltinFunctions.SUM, new ConstantExpression(index));
	}

	@Test
	public void testFunctionAndExpression() {
		AggregationFunction func = BuiltinFunctions.SUM;
		ConstantExpression expr = new ConstantExpression(1);
		AggregationExpression aggregation = new AggregationExpression(func, expr);
		Assert.assertEquals(func, aggregation.getFunction());
		Assert.assertEquals(expr, aggregation.getPreprocessing());
	}

	@Test
	public void shouldAggregate() {
		JsonNode result = new AggregationExpression(BuiltinFunctions.AVERAGE).evaluate(createArrayNode(2, 4),
			this.context);
		Assert.assertEquals(JsonUtil.NODE_FACTORY.numberNode(3.0), result);
	}
}
