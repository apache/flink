package eu.stratosphere.sopremo.io;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class JsonGeneratorTest {
	private static ObjectNode obj;

	private static ArrayNode arr;

	@BeforeClass
	public static void setUpClass() {
		obj = new ObjectNode();
		final ArrayNode friends = new ArrayNode();

		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 1")).put("age", IntNode.valueOf(20))
			.put("male", BooleanNode.TRUE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(30))
			.put("male", BooleanNode.FALSE));
		friends.add(new ObjectNode().put("name", TextNode.valueOf("testfriend 2")).put("age", IntNode.valueOf(40))
			.put("male", NullNode.getInstance()));
		friends.add(NullNode.getInstance());

		obj.put("name", TextNode.valueOf("Person 1")).put("age", IntNode.valueOf(25)).put("male", BooleanNode.TRUE)
			.put("friends", friends);

		arr = new ArrayNode();
		arr.add(obj);
		arr.add(NullNode.getInstance());
		arr.add(obj);
	}

	@Test
	public void testGeneration() {
		try {
			final File file = File.createTempFile("test", "json");
			final JsonGenerator gen = new JsonGenerator(file);
			// gen.writeStartArray();
			gen.writeTree(arr);
			// gen.writeEndArray();
			gen.close();
			final JsonParser parser = new JsonParser(new FileReader(file));
			parser.readValueAsTree();
			Assert.assertEquals(NullNode.getInstance(), parser.readValueAsTree());
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void shouldGenerateGivenFile() {
		try {
			final JsonParser parser = new JsonParser(new URL(
				SopremoTest.getResourcePath("SopremoTestPlan/test.json")));
			final File file = File.createTempFile("test", "json");
			final JsonGenerator gen = new JsonGenerator(file);
			gen.writeStartArray();
			while (!parser.checkEnd())
				gen.writeTree(parser.readValueAsTree());
			gen.writeEndArray();
			gen.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
