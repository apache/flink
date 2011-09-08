package eu.stratosphere.usecase.cleansing;
import static eu.stratosphere.sopremo.JsonUtil.*;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.junit.Test;

import eu.stratosphere.sopremo.BuiltinFunctions;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpressionTest;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

public class AggExpressionTest extends EvaluableExpressionTest<AggExpression> {
	@Override
	protected AggExpression createDefaultInstance(int index) {
		return new AggExpression(new ConstantExpression(index), EvaluationExpression.NULL);
	}

	@Test
	public void testAggregation() {
		context.getFunctionRegistry().register(BuiltinFunctions.class);
		
		ArrayNode input = new ArrayNode(null);
		input.add(createObjectNode("key", 1, "value", 11));
		input.add(createObjectNode("key", 2, "value", 24));
		input.add(createObjectNode("key", 3, "value", 33));
		input.add(createObjectNode("key", 2, "value", 25));
		input.add(createObjectNode("key", 1, "value", 12));
		
		AggExpression aggExpression = new AggExpression(new ObjectAccess("key"), new FunctionCall("sum",
			new ArrayProjection(new ObjectAccess("value"))));

		JsonNode result = aggExpression.evaluate(input, context);

		ArrayNode expected = new ArrayNode(null);
		expected.add(createValueNode(23));
		expected.add(createValueNode(49));
		expected.add(createValueNode(33));

		Assert.assertEquals(expected, result);
	}
}
