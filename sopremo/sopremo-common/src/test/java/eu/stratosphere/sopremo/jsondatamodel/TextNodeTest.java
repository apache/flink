package eu.stratosphere.sopremo.jsondatamodel;

import junit.framework.Assert;
import eu.stratosphere.sopremo.type.TextNode;

public class TextNodeTest extends JsonNodeTest<TextNode> {

	@Override
	public void testValue() {
		final TextNode textnode = new TextNode("sample TextNode");
		Assert.assertEquals("sample TextNode", textnode.getTextValue());
	}
}
