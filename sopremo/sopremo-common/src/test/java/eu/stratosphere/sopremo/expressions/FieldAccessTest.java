package eu.stratosphere.sopremo.expressions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;

public class FieldAccessTest extends EvaluableExpressionTest<FieldAccess> {
	@Override
	protected FieldAccess createDefaultInstance(int index) {
		return new FieldAccess(String.valueOf(index));
	}

	@Test
	public void shouldAccessFieldOfSingleObject() {
		JsonNode result = new FieldAccess("fieldName").evaluate(createObjectNode("fieldName", 42, "fieldName2", 12),
			context);
		Assert.assertEquals(createValueNode(42), result);
	}

	@Test
	public void shouldAccessFieldOfArray() {
		JsonNode result = new FieldAccess("fieldName").evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@Test
	public void shouldAccessFieldOfStreamArray() {
		JsonNode result = new FieldAccess("fieldName").evaluate(
			createStreamArray(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createStreamArray(1, 2, 3), result);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoObjectOrArray() {
		new FieldAccess("fieldName").evaluate(createValueNode(42), context);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfArrayOfPrimitives() {
		new FieldAccess("fieldName").evaluate(createArrayNode(1, 2, 3), context);
	}

}
