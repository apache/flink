package eu.stratosphere.sopremo.jsondatamodel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

	@After
	public void tearDown() {
		try {
			this.outStream.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
