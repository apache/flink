package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;

import java.util.Arrays;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

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

		equalVerifier.withPrefabValues(IJsonNode.class, IntNode.valueOf(23), IntNode.valueOf(42));
	}

	@Test
	public void shouldPerformBatch() {
		final BatchAggregationExpression batch = new BatchAggregationExpression(DefaultFunctions.SUM);
		batch.add(DefaultFunctions.AVERAGE);
		batch.add(DefaultFunctions.AVERAGE, new ArithmeticExpression(EvaluationExpression.VALUE,
			ArithmeticOperator.MULTIPLICATION, EvaluationExpression.VALUE));
		final IJsonNode result = batch.evaluate(createArrayNode(2, 3, 4, 5, 1), null, this.context);

		Assert.assertTrue(result instanceof IArrayNode);
		final IArrayNode resultArray = (IArrayNode) result;
		final double[] doubleResult = new double[resultArray.size()];
		for (int index = 0; index < doubleResult.length; index++) {
			Assert.assertTrue(resultArray.get(index) instanceof INumericNode);
			doubleResult[index] = ((INumericNode) resultArray.get(index)).getDoubleValue();
		}

		final double[] expected = { 1 + 2 + 3 + 4 + 5,
			(double) (1 + 2 + 3 + 4 + 5) / 5,
			(double) (1 * 1 + 2 * 2 + 3 * 3 + 4 * 4 + 5 * 5) / 5 };
		Assert.assertTrue(Arrays.equals(expected, doubleResult));
	}

	@Test
	public void shouldReuseTarget() {
		final ArrayNode target = new ArrayNode();
		final BatchAggregationExpression batch = new BatchAggregationExpression(DefaultFunctions.SUM);
		batch.add(DefaultFunctions.AVERAGE);
		batch.add(DefaultFunctions.AVERAGE, new ArithmeticExpression(EvaluationExpression.VALUE,
			ArithmeticOperator.MULTIPLICATION, EvaluationExpression.VALUE));
		final IJsonNode result = batch.evaluate(createArrayNode(2, 3, 4, 5, 1), target, this.context);

		Assert.assertSame(target, result);
	}

	@Test
	public void shouldNotReuseTargetWithWrongType() {
		final ObjectNode target = new ObjectNode();
		final BatchAggregationExpression batch = new BatchAggregationExpression(DefaultFunctions.SUM);
		batch.add(DefaultFunctions.AVERAGE);
		batch.add(DefaultFunctions.AVERAGE, new ArithmeticExpression(EvaluationExpression.VALUE,
			ArithmeticOperator.MULTIPLICATION, EvaluationExpression.VALUE));
		final IJsonNode result = batch.evaluate(createArrayNode(2, 3, 4, 5, 1), target, this.context);

		Assert.assertNotSame(target, result);
	}
}
