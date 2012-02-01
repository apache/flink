package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

public class TernaryExpressionTest extends EvaluableExpressionTest<TernaryExpression> {

	@Override
	protected TernaryExpression createDefaultInstance(final int index) {
		return new TernaryExpression(new ConstantExpression(BooleanNode.TRUE), new ConstantExpression(
			IntNode.valueOf(index)), new ConstantExpression(IntNode.valueOf(index)));
	}

	@Test
	public void shouldEvaluateIfExpIfClauseIsTrue() {
		final IJsonNode result = new TernaryExpression(new InputSelection(0),
			new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(1))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(IntNode.valueOf(0), result);
	}

	@Test
	public void shouldEvaluateThenExpIfClauseIsTrue() {
		final IJsonNode result = new TernaryExpression(new InputSelection(1),
			new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(1))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(IntNode.valueOf(1), result);
	}

	@Test
	public void shouldBeNullIfThenExprIsEmpty() {
		final IJsonNode result = new TernaryExpression(new InputSelection(1),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(
			createArrayNode(BooleanNode.TRUE, BooleanNode.FALSE), this.context);

		Assert.assertEquals(NullNode.getInstance(), result);
	}

	@Test
	public void shouldEvaluateIntNodes() {
		final IJsonNode result = new TernaryExpression(new ConstantExpression(IntNode.valueOf(0)),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(NullNode.getInstance(), result);
	}

	@Test
	public void shouldEvaluateTextNodes() {
		final IJsonNode result = new TernaryExpression(new ConstantExpression(TextNode.valueOf("a")),
			new ConstantExpression(IntNode.valueOf(0))).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(IntNode.valueOf(0), result);
	}
}
