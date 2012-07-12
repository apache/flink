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
		final BooleanExpression[] params = new BooleanExpression[index + 1];
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

}
