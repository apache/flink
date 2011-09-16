package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class AndExpressionTest extends BooleanExpressionTest<AndExpression> {

	@Override
	protected AndExpression createDefaultInstance(final int index) {

		switch (index) {
		case 0: {
			return new AndExpression(this.TRUE);
		}
		case 1: {
			return new AndExpression(this.TRUE, this.TRUE);
		}
		case 2: {
			return new AndExpression(this.TRUE, this.TRUE, this.TRUE);
		}
		}

		return super.createDefaultInstance(index);
	}

	@Test
	public void shouldBeTrueIfAllExprAreTrue() {
		final JsonNode result = new AndExpression(this.TRUE, this.TRUE, this.TRUE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfOneExprIsFalse() {
		final JsonNode result = new AndExpression(this.TRUE, this.FALSE, this.TRUE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new AndExpression();
	}
}
