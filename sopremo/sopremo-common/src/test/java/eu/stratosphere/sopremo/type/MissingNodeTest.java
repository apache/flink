package eu.stratosphere.sopremo.type;

import org.junit.Assert;
import org.junit.Test;

public class MissingNodeTest {

	@Test
	public void shouldBeEqualWithItself() {
		Assert.assertEquals(MissingNode.getInstance(), MissingNode.getInstance());
	}

	@Test
	public void shouldNotBeEqualWithAnotherInstance() {
		Assert.assertFalse(MissingNode.getInstance().equals(new MissingNode()));
	}

	@Test
	public void shouldHaveACompletelyZeroedKey() {
		final byte[] target = new byte[5];
		final byte[] expected = new byte[] { 0, 0, 0, 0, 0 };

		MissingNode.getInstance().copyNormalizedKey(target, 0, 5);

		Assert.assertArrayEquals(expected, target);
	}
}
