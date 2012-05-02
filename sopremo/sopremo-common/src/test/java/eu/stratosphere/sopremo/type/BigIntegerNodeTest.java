package eu.stratosphere.sopremo.type;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class BigIntegerNodeTest extends JsonNodeTest<BigIntegerNode> {

	@Test
	public void shouldNormalizeCorrectly() {
		BigIntegerNode node = new BigIntegerNode(BigInteger.valueOf(42));
		byte[] target = new byte[5];
		node.copyNormalizedKey(target, 0, 5);

		byte[] expected = { (byte) 0, (byte) 0, (byte) 0, (byte) 1, (byte) 42 };

		Assert.assertTrue(Arrays.equals(expected, target));
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		return BigIntegerNode.valueOf(BigInteger.valueOf(-42));
	}

	@Override
	protected IJsonNode higherNode() {
		return BigIntegerNode.valueOf(BigInteger.valueOf(Long.MAX_VALUE));
	}

}
