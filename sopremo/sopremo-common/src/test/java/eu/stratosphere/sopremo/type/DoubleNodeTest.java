package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

public class DoubleNodeTest extends JsonNodeTest<DoubleNode> {

	@Override
	public void testValue() {
		final DoubleNode doublenode = new DoubleNode(23.42);
		Assert.assertEquals(23.42, doublenode.getDoubleValue());
	}

	@Override
	protected IJsonNode lowerNode() {
		return DoubleNode.valueOf(42.67);
	}

	@Override
	protected IJsonNode higherNode() {
		return DoubleNode.valueOf(43.42);
	}

}
