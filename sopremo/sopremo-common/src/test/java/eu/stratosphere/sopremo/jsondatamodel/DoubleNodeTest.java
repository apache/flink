package eu.stratosphere.sopremo.jsondatamodel;

import junit.framework.Assert;
import eu.stratosphere.sopremo.type.DoubleNode;

public class DoubleNodeTest extends JsonNodeTest<DoubleNode> {

	@Override
	public void testValue() {
		final DoubleNode doublenode = new DoubleNode(23.42);
		Assert.assertEquals(23.42, doublenode.getDoubleValue());
	}

}
