package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

public class ArrayProjectionTest extends EvaluableExpressionTest<LazyArrayProjection> {
	@Override
	protected LazyArrayProjection createDefaultInstance(int index) {
		return new LazyArrayProjection(new ObjectAccess(String.valueOf(index)));
	}

	@Test
	public void shouldAccessFieldOfArray() {
		JsonNode result = new LazyArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@Test
	public void shouldAccessFieldOfStreamArray() {
		JsonNode result = new LazyArrayProjection(new ObjectAccess("fieldName")).evaluate(
			createStreamArray(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createStreamArray(1, 2, 3), result);
	}

}
