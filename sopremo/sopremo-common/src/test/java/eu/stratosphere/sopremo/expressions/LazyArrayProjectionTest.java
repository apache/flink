package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

public class LazyArrayProjectionTest extends EvaluableExpressionTest<LazyArrayProjection> {
	@Override
	protected LazyArrayProjection createDefaultInstance(final int index) {
		return new LazyArrayProjection(new ObjectAccess(String.valueOf(index)));
	}

	@Test
	public void shouldAccessFieldOfArray() {
		final JsonNode result = new LazyArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@Test
	public void shouldAccessFieldOfStreamArray() {
		final JsonNode result = new LazyArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createStreamArray(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createStreamArray(1, 2, 3), result);
	}

}
