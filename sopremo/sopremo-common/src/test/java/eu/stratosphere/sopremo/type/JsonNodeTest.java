package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.reflect.BoundTypeUtil;

public abstract class JsonNodeTest<T extends JsonNode> {
	// generic tests for every JsonNode

	T node;

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

}
