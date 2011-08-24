package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Test;

public class TernaryExpressionTest extends EvaluableExpressionTest<TernaryExpression> {

	@Override
	protected TernaryExpression createDefaultInstance(int index) {
		return new TernaryExpression(new ConstantExpression(BooleanNode.TRUE), new ConstantExpression(
			IntNode.valueOf(index)), new ConstantExpression(IntNode.valueOf(index)));
	}

	@Test
	public void shouldEvaluateIfExpIfClauseIsTrue() {
		final JsonNode result = new TernaryExpression(new InputSelection(0),
			new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(1))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(IntNode.valueOf(0), result);
	}

	@Test
	public void shouldEvaluateThenExpIfClauseIsTrue() {
		final JsonNode result = new TernaryExpression(new InputSelection(1),
			new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(1))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(IntNode.valueOf(1), result);
	}

	@Test
	public void shouldBeNullIfThenExprIsEmpty() {
		final JsonNode result = new TernaryExpression(new InputSelection(1),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(NullNode.getInstance(), result);
	}

	@Test
	public void shouldEvaluateIntNodes() {
		final JsonNode result = new TernaryExpression(new ConstantExpression(IntNode.valueOf(0)),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(NullNode.getInstance(), result);
	}

	@Test
	public void shouldEvaluateTextNodes() {
		final JsonNode result = new TernaryExpression(new ConstantExpression(TextNode.valueOf("a")),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(IntNode.valueOf(0), result);
	}
}
