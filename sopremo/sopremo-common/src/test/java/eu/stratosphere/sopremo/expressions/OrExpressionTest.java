package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class OrExpressionTest extends BooleanExpressionTest<OrExpression> {

	@Override
	protected OrExpression createDefaultInstance(final int index) {
		final EvaluationExpression[] params = new EvaluationExpression[index + 1];
		Arrays.fill(params, TRUE);
		return new OrExpression(params);
	}

	@Test
	public void shouldBeTrueIfOneExprIsTrue() {
		final IJsonNode result = new OrExpression(BooleanExpressionTest.FALSE, BooleanExpressionTest.TRUE,
			BooleanExpressionTest.FALSE).evaluate(IntNode.valueOf(42),
			null, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfNoExprIsTrue() {
		final IJsonNode result = new OrExpression(BooleanExpressionTest.FALSE, BooleanExpressionTest.FALSE,
			BooleanExpressionTest.FALSE).evaluate(IntNode.valueOf(42),
			null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new OrExpression();
	}

}
