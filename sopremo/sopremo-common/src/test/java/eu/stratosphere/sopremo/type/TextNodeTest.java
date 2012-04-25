package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

public class TextNodeTest extends JsonNodeTest<TextNode> {

	@Override
	public void testValue() {
		final TextNode textnode = new TextNode("sample TextNode");
		Assert.assertEquals("sample TextNode", textnode.getTextValue());
	}
}
