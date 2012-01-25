package eu.stratosphere.sopremo.jsondatamodel;

import junit.framework.Assert;
import eu.stratosphere.sopremo.type.IntNode;

public class IntNodeTest extends JsonNodeTest<IntNode> {

	@Override
	public void testValue() {
		final IntNode intnode = new IntNode(23);
		Assert.assertEquals(23, intnode.getIntValue());
	}

}
