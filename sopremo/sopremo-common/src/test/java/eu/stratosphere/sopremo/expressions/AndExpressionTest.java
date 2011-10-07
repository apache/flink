package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class AndExpressionTest extends BooleanExpressionTest<AndExpression> {

	@Override
	protected AndExpression createDefaultInstance(int index) {
		EvaluationExpression[] params = new EvaluationExpression[index + 1];
		Arrays.fill(params, TRUE);
		return new AndExpression(params);
	}

	@Test
	public void shouldBeTrueIfAllExprAreTrue() {
		final JsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.TRUE, BooleanExpressionTest.TRUE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfOneExprIsFalse() {
		final JsonNode result = new AndExpression(BooleanExpressionTest.TRUE, BooleanExpressionTest.FALSE, BooleanExpressionTest.TRUE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new AndExpression();
	}
}
