package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.function.Aggregation;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;

public class AggregationExpressionTest extends EvaluableExpressionTest<AggregationExpression> {
	@Override
	protected AggregationExpression createDefaultInstance(final int index) {
		return new AggregationExpression(CoreFunctions.SUM, new ConstantExpression(index));
	}

	@Test
	public void testFunctionAndExpression() {
		final Aggregation func = CoreFunctions.SUM;
		final ConstantExpression expr = new ConstantExpression(1);
		final AggregationExpression aggregation = new AggregationExpression(func, expr);
		Assert.assertEquals(func, aggregation.getFunction());
		Assert.assertEquals(expr, aggregation.getPreprocessing());
	}

	@Test
	public void shouldAggregate() {
		final IJsonNode result = new AggregationExpression(CoreFunctions.AVERAGE).evaluate(createArrayNode(2, 4),
			null, this.context);
		Assert.assertTrue(result instanceof INumericNode);
		Assert.assertEquals(3.0, ((INumericNode) result).getDoubleValue());
	}
}
