package eu.stratosphere.sopremo.jsondatamodel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SerializableTest {

	private static ObjectNode obj;

	private ByteArrayOutputStream byteArray;

	private DataOutputStream outStream;

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

		obj.put("name", TextNode.valueOf("Person 1")).put("age", IntNode.valueOf(25)).put("male", BooleanNode.TRUE)
			.put("friends", friends);
	}

	@Before
	public void setUp() {
		this.byteArray = new ByteArrayOutputStream();
		this.outStream = new DataOutputStream(this.byteArray);
	}

	@Test
	public void shouldDeAndSerializeNodesCorrectly() {
		try {
			obj.write(this.outStream);
		} catch (final IOException e) {
			e.printStackTrace();
		}

		final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
		final DataInputStream inStream = new DataInputStream(byteArrayIn);

		final ObjectNode target = new ObjectNode();

		try {
			target.read(inStream);
			inStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@Test
	public void shouldDeAndSerializeEmptyNode() {
		obj = new ObjectNode();

		try {
			obj.write(this.outStream);
		} catch (final IOException e) {
			e.printStackTrace();
		}

		final ObjectNode target = new ObjectNode();

		final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
		final DataInputStream inStream = new DataInputStream(byteArrayIn);

		try {
			target.read(inStream);
			inStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@Test
	public void shouldSerializeAndDeserializeGivenFile() {

		final ArrayNode array = new ArrayNode();

		try {
			final JsonParser parser = new JsonParser(new URL(
				SopremoTest.getResourcePath("SopremoTestPlan/test.json")));
			final File file = File.createTempFile("test", "json");

			while (!parser.checkEnd())
				array.add(parser.readValueAsTree());

			array.write(this.outStream);

			final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
			final DataInputStream inStream = new DataInputStream(byteArrayIn);

			final ArrayNode target = new ArrayNode();
			target.read(inStream);

			// for watching the output
			final JsonGenerator gen = new JsonGenerator(file);
			gen.writeStartArray();
			gen.writeTree(target);
			gen.writeEndArray();
			gen.close();

			Assert.assertEquals(array, target);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	@After
	public void tearDown() {
		try {
			this.outStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
