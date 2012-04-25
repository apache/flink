package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Test;

public class ObjectMapperTest {

	@Test
	public void shouldMapArray() {
		final Object[] array = { "field1", 1 };
		final JsonNode node = new JavaToJsonMapper().valueToTree(array);
		Assert.assertEquals(new ArrayNode().add(new TextNode("field1")).add(new IntNode(1)), node);
	}

	@Test
	public void shouldMapNestedArray() {
		final Object[] root = new Object[2];
		final Object[] array1 = { "field1", 1 };
		final Object[] array2 = { "field2", 2 };
		root[0] = array1;
		root[1] = array2;
		final JsonNode node = new JavaToJsonMapper().valueToTree(root);

		Assert.assertEquals(
			new ArrayNode().add(new ArrayNode().add(new TextNode("field1")).add(new IntNode(1)))
				.add(new ArrayNode().add(new TextNode("field2")).add(new IntNode(2))),
			node);
	}

	@Test
	public void shouldMapIntArray() {
		final int[] root = { 1, 2, 3 };
		final JsonNode node = new JavaToJsonMapper().valueToTree(root);
		Assert.assertEquals(new ArrayNode().add(new IntNode(1)).add(new IntNode(2)).add(new IntNode(3)),
			node);
	}
}
