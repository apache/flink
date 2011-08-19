package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

public class OrExpressionTest extends BooleanExpressionTest<OrExpression> {
	@Override
	protected OrExpression createDefaultInstance(int index) {
		switch (index) {
		case 0:
			return new OrExpression(TRUE);
		case 1:
			return new OrExpression(FALSE);
		case 2:
			return new OrExpression(TRUE, FALSE);
		}
		return super.createDefaultInstance(index);
	}

	

	@Test
	public void ShouldReturnTrueIfOneExprIsTrue() {
		final JsonNode result = new OrExpression(TRUE).evaluate(IntNode.valueOf(42), this.context);
		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void ShouldReturnFalseIfAllExprAreFalse() {
		final JsonNode result = new OrExpression(FALSE).evaluate(IntNode.valueOf(42), this.context);
		Assert.assertEquals(BooleanNode.FALSE, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void ShouldThrowExceptionOnNoArguments() {
		new OrExpression();
	}
}
