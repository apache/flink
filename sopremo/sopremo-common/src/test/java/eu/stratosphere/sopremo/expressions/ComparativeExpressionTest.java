package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

public class ComparativeExpressionTest extends EvaluableExpressionTest<ComparativeExpression> {
	@Override
	protected ComparativeExpression createDefaultInstance(final int index) {
		return new ComparativeExpression(new UnaryExpression(new ConstantExpression(IntNode.valueOf(index))),
			BinaryOperator.NOT_EQUAL, new UnaryExpression(new ConstantExpression(IntNode.valueOf(index + 1))));
	}

	@Ignore
	public JsonNode evaluate(final JsonNode expr1, final BinaryOperator op, final JsonNode expr2) {
		return new ComparativeExpression(new InputSelection(0), op, new InputSelection(1)).evaluate(
			createArrayNode(expr1, expr2), this.context);

	}

	@Test(expected = EvaluationException.class)
	public void shouldThrowExceptionWhenComparingNumericNodeWithTextNode() {
		this.evaluate(IntNode.valueOf(42), BinaryOperator.EQUAL, TextNode.valueOf("42"));

	}
}
