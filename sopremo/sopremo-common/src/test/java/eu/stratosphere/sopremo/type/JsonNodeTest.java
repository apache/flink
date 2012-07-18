package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.util.reflect.BoundTypeUtil;

public abstract class JsonNodeTest<T extends IJsonNode> {
	// generic tests for every JsonNode

	protected T node;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		try {
			this.node = (T) BoundTypeUtil.getBindingOfSuperclass(this.getClass(), JsonNodeTest.class).getParameters()[0]
				.getType().newInstance();
		} catch (final InstantiationException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testToString() {
		Assert.assertNotSame(
			"builder did not write anything - override this test if it is indeed the desired behavior", "",
			this.node.toString());
	}

	@Test
	public void testTypeNumber() {
		Assert.assertNotNull("every JsonNode must have a TypeNumber", this.node.getType().ordinal());
	}

	@Test
	public abstract void testValue();

	protected abstract IJsonNode lowerNode();

	protected abstract IJsonNode higherNode();

	@Test
	public void shouldNormalizeKeys() {
		int lenght = 100;

		final IJsonNode lower = this.lowerNode();
		final IJsonNode higher = this.higherNode();

		lenght = higher.getMaxNormalizedKeyLen() < lenght ? higher.getMaxNormalizedKeyLen() : lenght;

		final byte[] lowerTarget = new byte[lenght];
		final byte[] higherTarget = new byte[lenght];

		lower.copyNormalizedKey(lowerTarget, 0, lenght);
		higher.copyNormalizedKey(higherTarget, 0, lenght);

		for (int i = 0; i < lenght; i++) {
			final byte lowerByte = lowerTarget[i];
			final byte higherByte = higherTarget[i];

			if (lowerByte < higherByte)
				break;

			Assert.assertTrue(lowerByte == higherByte);
		}
	}
}
