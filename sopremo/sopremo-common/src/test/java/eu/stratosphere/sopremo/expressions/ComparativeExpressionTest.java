package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;

public class ComparativeExpressionTest extends EvaluableExpressionTest<ComparativeExpression> {

	@Override
	protected ComparativeExpression createDefaultInstance(int index) {
		return new ComparativeExpression(new UnaryExpression(new ConstantExpression(IntNode.valueOf(index))),
			BinaryOperator.NOT_EQUAL, new UnaryExpression(new ConstantExpression(IntNode.valueOf(index + 1))));
		// return super.createDefaultInstance(index);
	}

	@Ignore
	public JsonNode Evaluate(BinaryOperator op) {
		return new ComparativeExpression(new InputSelection(0), op, new InputSelection(1)).evaluate(
			createArrayNode(IntNode.valueOf(42), IntNode.valueOf(23)), this.context);

	}

	@Test
	public void ShouldReturnTrueIfEvaluationReturnsTrue() {
		JsonNode result = this.Evaluate(BinaryOperator.GREATER);
		Assert.assertEquals(BooleanNode.TRUE, result);
		result = this.Evaluate(BinaryOperator.GREATER_EQUAL);
		Assert.assertEquals(BooleanNode.TRUE, result);
		result = this.Evaluate(BinaryOperator.NOT_EQUAL);
		Assert.assertEquals(BooleanNode.TRUE, result);

	}

	@Test
	public void ShouldReturnFalseIfEvaluationReturnsFalse() {
		JsonNode result = this.Evaluate(BinaryOperator.EQUAL);
		Assert.assertEquals(BooleanNode.FALSE, result);
		result = this.Evaluate(BinaryOperator.LESS);
		Assert.assertEquals(BooleanNode.FALSE, result);
		result = this.Evaluate(BinaryOperator.LESS_EQUAL);
		Assert.assertEquals(BooleanNode.FALSE, result);
	}
}
