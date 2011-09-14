package eu.stratosphere.sopremo.expressions;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import static eu.stratosphere.sopremo.JsonUtil.createValueNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class FieldAccessTest extends EvaluableExpressionTest<ObjectAccess> {
	@Override
	protected ObjectAccess createDefaultInstance(final int index) {
		return new ObjectAccess(String.valueOf(index));
	}

	@Test
	public void shouldAccessFieldOfSingleObject() {
		final JsonNode result = new ObjectAccess("fieldName").evaluate(
			createObjectNode("fieldName", 42, "fieldName2", 12),
			this.context);
		Assert.assertEquals(createValueNode(42), result);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfArrayOfPrimitives() {
		new ObjectAccess("fieldName").evaluate(createArrayNode(1, 2, 3), this.context);
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoObjectOrArray() {
		new ObjectAccess("fieldName").evaluate(createValueNode(42), this.context);
	}

}
