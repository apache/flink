package eu.stratosphere.sopremo.jsondatamodel;

import junit.framework.Assert;

public class IntNodeTest extends JsonNodeTest<IntNode> {

	@Override
	public void testValue() {
		final IntNode intnode = new IntNode(23);
		Assert.assertEquals(Integer.valueOf(23), intnode.getIntValue());
	}

}
