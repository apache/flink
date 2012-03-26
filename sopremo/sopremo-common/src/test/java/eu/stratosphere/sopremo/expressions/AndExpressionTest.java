package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class AndExpressionTest extends BooleanExpressionTest<AndExpression> {

	@Override
	protected AndExpression createDefaultInstance(final int index) {
		final EvaluationExpression[] params = new EvaluationExpression[index + 1];
		Arrays.fill(params, TRUE);
		return new AndExpression(params);
	}

	@Test
	public void shouldBeTrueIfAllExprAreTrue() {
		final IJsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.TRUE,
			BooleanExpressionTest.TRUE).evaluate(IntNode.valueOf(42),
			null, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfOneExprIsFalse() {
		final IJsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.FALSE,
			BooleanExpressionTest.TRUE).evaluate(IntNode.valueOf(42),
			null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new AndExpression();
	}

	@Ignore
	@Test
	public void shouldReuseTargetIfTrue() {
		IJsonNode target = BooleanNode.FALSE;
		final IJsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.TRUE).evaluate(
			IntNode.valueOf(42),
			target, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
		Assert.assertSame(target, result);
	}

	@Ignore
	@Test
	public void shouldReuseTargetIfFalse() {
		IJsonNode target = BooleanNode.TRUE;
		final IJsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.FALSE).evaluate(
			IntNode.valueOf(42),
			target, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
		Assert.assertSame(target, result);
	}
}
