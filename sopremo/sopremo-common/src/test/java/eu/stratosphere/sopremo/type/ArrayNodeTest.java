package eu.stratosphere.sopremo.type;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

public class ArrayNodeTest extends ArrayNodeBaseTest<ArrayNode> {

	@Override
	public void initArrayNode() {
		this.node = new ArrayNode();
		final int numberOfNodes = 10;

		for (int i = 0; i < numberOfNodes; i++)
			this.node.add(i, IntNode.valueOf(i));
	}

	@Test
	public void shouldReturnCorrectSubarray() {
		final int numberOfNodesInSubarray = 5;
		final int startIndex = 3;
		final IArrayNode result = new ArrayNode();

		for (int i = 0; i < numberOfNodesInSubarray; i++)
			result.add(i, IntNode.valueOf(startIndex + i));

		Assert.assertEquals(result, this.node.subArray(startIndex, startIndex + numberOfNodesInSubarray));
	}

	@Test
	public void shouldCreateNewArrayNodeFromIterator() {
		final Iterator<IJsonNode> it = this.node.iterator();
		final ArrayNode newArray = ArrayNode.valueOf(it);

		Assert.assertEquals(this.node, newArray);
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		return new ArrayNode(IntNode.valueOf(42), TextNode.valueOf("1 lower Node"));
	}

	@Override
	protected IJsonNode higherNode() {
		return new ArrayNode(IntNode.valueOf(42), TextNode.valueOf("2 higher Node"));
	}

}
