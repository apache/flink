package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;
import org.junit.Test;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;

public class AndExpressionTest extends BooleanExpressionTest<AndExpression> {

	@Override
	protected AndExpression createDefaultInstance(int index) {

		switch (index) {
		case 0: {
			return new AndExpression(TRUE);
		}
		case 1: {
			return new AndExpression(TRUE, TRUE);
		}
		case 2: {
			return new AndExpression(TRUE, TRUE, TRUE);
		}
		}

		return super.createDefaultInstance(index);
	}

	@Test
	public void shouldBeTrueIfAllExprAreTrue() {
		final JsonNode result = new AndExpression(TRUE, TRUE, TRUE).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfOneExprIsFalse() {
		final JsonNode result = new AndExpression(TRUE, FALSE, TRUE).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new AndExpression();
	}
}
