package eu.stratosphere.sopremo.expressions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;

public class FieldAccessTest extends EvaluableExpressionTest<ObjectAccess> {
	@Override
	protected ObjectAccess createDefaultInstance(int index) {
		return new ObjectAccess(String.valueOf(index));
	}

	@Test
	public void shouldAccessFieldOfSingleObject() {
		JsonNode result = new ObjectAccess("fieldName").evaluate(createObjectNode("fieldName", 42, "fieldName2", 12),
			context);
		Assert.assertEquals(createValueNode(42), result);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoObjectOrArray() {
		new ObjectAccess("fieldName").evaluate(createValueNode(42), context);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfArrayOfPrimitives() {
		new ObjectAccess("fieldName").evaluate(createArrayNode(1, 2, 3), context);
	}

}
