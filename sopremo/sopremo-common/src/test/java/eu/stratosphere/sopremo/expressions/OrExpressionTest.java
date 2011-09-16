package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class OrExpressionTest extends BooleanExpressionTest<OrExpression> {

	@Override
	protected OrExpression createDefaultInstance(final int index) {

		switch (index) {
		case 0: {
			return new OrExpression(this.TRUE);
		}
		case 1: {
			return new OrExpression(this.TRUE, this.TRUE);
		}
		case 2: {
			return new OrExpression(this.TRUE, this.TRUE, this.TRUE);
		}
		}

		return super.createDefaultInstance(index);
	}

	@Test
	public void shouldBeTrueIfOneExprIsTrue() {
		final JsonNode result = new OrExpression(this.FALSE, this.TRUE, this.FALSE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfNoExprIsTrue() {
		final JsonNode result = new OrExpression(this.FALSE, this.FALSE, this.FALSE).evaluate(IntNode.valueOf(42),
			this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new OrExpression();
	}

}
