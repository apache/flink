package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.IJsonNode;

public class LazyArrayProjectionTest extends EvaluableExpressionTest<ArrayProjection> {
	@Override
	protected ArrayProjection createDefaultInstance(final int index) {
		return new ArrayProjection(new ObjectAccess(String.valueOf(index)));
	}

	@Test
	public void shouldAccessFieldOfArray() {
		final IJsonNode result = new ArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@Test
	public void shouldAccessFieldOfStreamArray() {
		final IJsonNode result = new ArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

}
