package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

public class DoubleNodeTest extends JsonNodeTest<DoubleNode> {

	@Override
	public void testValue() {
		final DoubleNode doublenode = new DoubleNode(23.42);
		Assert.assertEquals(23.42, doublenode.getDoubleValue());
	}

}
