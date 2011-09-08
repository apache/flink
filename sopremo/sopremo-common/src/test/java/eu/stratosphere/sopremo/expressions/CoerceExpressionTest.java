package eu.stratosphere.sopremo.expressions;
import static eu.stratosphere.sopremo.JsonUtil.*;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Test;

import eu.stratosphere.sopremo.CoercionException;

public class CoerceExpressionTest extends EvaluableExpressionTest<CoerceExpression> {

	@Override
	protected CoerceExpression createDefaultInstance(int index) {
		switch (index) {
		case 0: {
			return new CoerceExpression(BooleanNode.class);
		}
		case 1: {
			return new CoerceExpression(NumericNode.class);
		}
		case 2: {
			return new CoerceExpression(TextNode.class);
		}
		}

		return new CoerceExpression(JsonNode.class);

	}

	@Test
	public void shouldChangeTypeOfIntToText() {
		final JsonNode result = new CoerceExpression(TextNode.class).evaluate(IntNode.valueOf(42), this.context);

		Assert.assertEquals(TextNode.valueOf("42"), result);
	}

	@Test
	public void shouldChangeTypeOfTextInterpretedNumberToInt() {
		final JsonNode result = new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("42"), this.context);

		Assert.assertEquals(IntNode.valueOf(42), result);
	}

	@Test
	public void shouldOnlyChangeOuterType(){
		final JsonNode result = new CoerceExpression(ArrayNode.class).evaluate(createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), this.context);
		
		Assert.assertEquals(createArrayNode(IntNode.valueOf(42), BooleanNode.TRUE), result);
	}
	
	@Test(expected = CoercionException.class)
	public void shouldThrowExceptionWhenChangeingTextToInt(){
		new CoerceExpression(IntNode.class).evaluate(TextNode.valueOf("testname"), this.context);
	}
}
