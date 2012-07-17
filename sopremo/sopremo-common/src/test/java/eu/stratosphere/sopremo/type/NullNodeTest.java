package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Test;

public class NullNodeTest {

	@Test
	public void shouldBeEqual() {
		Assert.assertEquals(NullNode.getInstance(), NullNode.getInstance());
	}

	@Test
	public void shouldNotBeEqualWithAnotherInstance() {
		Assert.assertFalse(NullNode.getInstance().equals(new NullNode()));
	}

	@Test
	public void shouldCanonicalizeToTheCommonInstance() {
		NullNode node = new NullNode();
		Assert.assertEquals(NullNode.getInstance(), node.canonicalize());
	}
}
