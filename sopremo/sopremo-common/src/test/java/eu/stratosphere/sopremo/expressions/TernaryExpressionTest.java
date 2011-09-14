package eu.stratosphere.sopremo.expressions;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
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
