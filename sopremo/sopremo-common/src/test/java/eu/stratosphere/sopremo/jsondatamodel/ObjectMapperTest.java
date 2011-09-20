package eu.stratosphere.sopremo.jsondatamodel;

import junit.framework.Assert;

import org.junit.Test;

public class ObjectMapperTest {

	@Test
	public void shouldMapArray() {
		Object[] array = { "field1", 1 };
		JsonNode node = new ObjectMapper().valueToTree(array);
		Assert.assertEquals(new ArrayNode().add(new TextNode("field1")).add(new IntNode(1)), node);
	}

	@Test
	public void shouldMapNestedArray() {
		Object[] root = new Object[2];
		Object[] array1 = { "field1", 1 };
		Object[] array2 = { "field2", 2 };
		root[0] = array1;
		root[1] = array2;
		JsonNode node = new ObjectMapper().valueToTree(root);

		Assert.assertEquals(
			new ArrayNode().add(new ArrayNode().add(new TextNode("field1")).add(new IntNode(1)))
				.add(new ArrayNode().add(new TextNode("field2")).add(new IntNode(2))),
			node);
	}
}
