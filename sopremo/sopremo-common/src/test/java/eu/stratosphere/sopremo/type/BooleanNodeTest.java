package eu.stratosphere.sopremo.type;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

public class BooleanNodeTest extends JsonNodeTest<BooleanNode> {

	@Test
	public void shouldNormalizeTRUECorrectly() {
		final BooleanNode node = BooleanNode.TRUE;
		final byte[] array = new byte[node.getMaxNormalizedKeyLen()];
		node.copyNormalizedKey(array, 0, 1);

		final byte[] expected = { (byte) 1 };

		Assert.assertTrue(Arrays.equals(array, expected));
	}

	@Test
	public void shouldNormalizeFALSECorrectly() {
		final BooleanNode node = BooleanNode.FALSE;
		final byte[] array = new byte[node.getMaxNormalizedKeyLen()];
		node.copyNormalizedKey(array, 0, 1);

		final byte[] expected = { (byte) 0 };

		Assert.assertTrue(Arrays.equals(array, expected));
	}

	@Test
	public void shouldFillBytearrayIfLenghtToBig() {
		final BooleanNode node = BooleanNode.TRUE;
		final byte[] array = new byte[4];
		node.copyNormalizedKey(array, 0, 4);

		final byte[] expected = { (byte) 1, (byte) 0, (byte) 0, (byte) 0 };

		Assert.assertTrue(Arrays.equals(array, expected));
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		return BooleanNode.FALSE;
	}

	@Override
	protected IJsonNode higherNode() {
		return BooleanNode.TRUE;
	}
}
