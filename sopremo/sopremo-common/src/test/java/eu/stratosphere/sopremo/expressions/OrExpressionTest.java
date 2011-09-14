package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
public class OrExpressionTest extends BooleanExpressionTest<OrExpression> {

	@Override
	protected OrExpression createDefaultInstance(int index) {

		switch (index) {
		case 0: {
			return new OrExpression(TRUE);
		}
		case 1: {
			return new OrExpression(TRUE, TRUE);
		}
		case 2: {
			return new OrExpression(TRUE, TRUE, TRUE);
		}
		}

		return super.createDefaultInstance(index);
	}

	@Test
	public void shouldBeTrueIfOneExprIsTrue() {
		final JsonNode result = new OrExpression(FALSE, TRUE, FALSE).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfNoExprIsTrue() {
		final JsonNode result = new OrExpression(FALSE, FALSE, FALSE).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty() {
		new OrExpression();
	}

}
