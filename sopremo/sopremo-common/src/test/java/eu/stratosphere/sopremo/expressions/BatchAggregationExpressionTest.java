package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;

public class BatchAggregationExpressionTest extends EvaluableExpressionTest<BatchAggregationExpression> {
	@Override
	protected BatchAggregationExpression createDefaultInstance(final int index) {
		switch (index) {
		case 0:
			return new BatchAggregationExpression(DefaultFunctions.AVERAGE);
		case 1:
			return new BatchAggregationExpression(DefaultFunctions.COUNT);
		case 2:
			return new BatchAggregationExpression(DefaultFunctions.FIRST);
		default:
			return new BatchAggregationExpression(DefaultFunctions.ALL);
		}

	}

	@Override
	protected void initVerifier(final EqualsVerifier<BatchAggregationExpression> equalVerifier) {
		super.initVerifier(equalVerifier);

		equalVerifier.withPrefabValues(JsonNode.class, IntNode.valueOf(23), IntNode.valueOf(42));
	}

	@Test
	public void should() {
		final BatchAggregationExpression batch = new BatchAggregationExpression(DefaultFunctions.SUM);
		batch.add(DefaultFunctions.AVERAGE);
		batch.add(DefaultFunctions.AVERAGE, new ArithmeticExpression(EvaluationExpression.VALUE,
			ArithmeticOperator.MULTIPLICATION, EvaluationExpression.VALUE));
		final JsonNode result = batch.evaluate(createArrayNode(2, 3, 4, 5, 1), this.context);
		final JsonNode[] expected = { new IntNode(1 + 2 + 3 + 4 + 5),
			new DoubleNode((double) (1 + 2 + 3 + 4 + 5) / 5),
			new DoubleNode((double) (1 * 1 + 2 * 2 + 3 * 3 + 4 * 4 + 5 * 5) / 5) };
		Assert.assertEquals(JsonUtil.asArray(expected), result);
	}
}
