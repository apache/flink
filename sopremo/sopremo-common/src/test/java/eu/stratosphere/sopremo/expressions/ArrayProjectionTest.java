package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createStreamArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.testing.AssertUtil;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

public class ArrayProjectionTest extends EvaluableExpressionTest<ArrayProjection> {
	@Override
	protected ArrayProjection createDefaultInstance(final int index) {
		return new ArrayProjection(new ObjectAccess(String.valueOf(index)));
	}

	@Test
	public void shouldAccessFieldOfArray() {
		final IJsonNode result = new ArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			null, this.context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldAccessFieldOfStreamArray() {
		final IJsonNode result = new ArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createStreamArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			null, this.context);
		Assert.assertFalse(result instanceof IArrayNode);
		Assert.assertTrue(result instanceof IStreamArrayNode);
		AssertUtil.assertIteratorEquals(createArrayNode(1, 2, 3).iterator(), ((Iterable<IJsonNode>) result).iterator());
	}

}
