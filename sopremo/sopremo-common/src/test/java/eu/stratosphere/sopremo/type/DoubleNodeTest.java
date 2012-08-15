package eu.stratosphere.sopremo.type;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

public class DoubleNodeTest extends JsonNodeTest<DoubleNode> {

	@Test
	public void shouldGenerateFullyZeroedNormalizedKey() {
		final int arraySize = 100;

		final DoubleNode node = DoubleNode.valueOf(42.42);

		final byte[] target = new byte[arraySize];
		final byte[] expected = new byte[arraySize];

		node.copyNormalizedKey(target, 0, arraySize);

		for (int i = 0; i < arraySize; i++)
			expected[i] = (byte) 0;

		Assert.assertTrue(Arrays.equals(expected, target));
	}

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
