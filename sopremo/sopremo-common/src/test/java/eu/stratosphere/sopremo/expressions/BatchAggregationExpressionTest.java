package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.aggregation.BuiltinFunctions;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;

public class BatchAggregationExpressionTest extends EvaluableExpressionTest<BatchAggregationExpression> {
	@Override
	protected BatchAggregationExpression createDefaultInstance(int index) {
		switch (index) {
		case 0:
			return new BatchAggregationExpression(BuiltinFunctions.AVERAGE);
		case 1:
			return new BatchAggregationExpression(BuiltinFunctions.COUNT);
		case 2:
			return new BatchAggregationExpression(BuiltinFunctions.FIRST);
		default:
			return new BatchAggregationExpression(BuiltinFunctions.ALL);
		}

	}

	@Override
	protected void initVerifier(EqualsVerifier<BatchAggregationExpression> equalVerifier) {
		super.initVerifier(equalVerifier);

		equalVerifier.withPrefabValues(JsonNode.class, IntNode.valueOf(23), IntNode.valueOf(42));
	}

	@Test
	public void should() {
		final BatchAggregationExpression batch = new BatchAggregationExpression(BuiltinFunctions.SUM);
		batch.add(BuiltinFunctions.AVERAGE);
		batch.add(BuiltinFunctions.AVERAGE, new ArithmeticExpression(EvaluationExpression.VALUE,
			ArithmeticOperator.MULTIPLICATION, EvaluationExpression.VALUE));
		JsonNode result = batch.evaluate(createArrayNode(2, 3, 4, 5, 1), this.context);
		JsonNode[] expected = { JsonUtil.NODE_FACTORY.numberNode(1 + 2 + 3 + 4 + 5),
			JsonUtil.NODE_FACTORY.numberNode((double)(1 + 2 + 3 + 4 + 5) / 5),
			JsonUtil.NODE_FACTORY.numberNode((double)(1 * 1 + 2 * 2 + 3 * 3 + 4 * 4 + 5 * 5) / 5) };
		Assert.assertEquals(JsonUtil.asArray(expected), result);
	}
}
