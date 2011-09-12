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
		ArrayNode friends = new ArrayNode();

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
		byteArray = new ByteArrayOutputStream();
		outStream = new DataOutputStream(byteArray);
	}

	@Test
	public void shouldDeAndSerializeNodesCorrectly() {
		try {
			obj.write(outStream);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(byteArray.toByteArray());
		DataInputStream inStream = new DataInputStream(byteArrayIn);

		ObjectNode target = new ObjectNode();

		try {
			target.read(inStream);
			inStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@Test
	public void shouldDeAndSerializeEmptyNode() {
		obj = new ObjectNode();

		try {
			obj.write(outStream);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ObjectNode target = new ObjectNode();

		ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(byteArray.toByteArray());
		DataInputStream inStream = new DataInputStream(byteArrayIn);

		try {
			target.read(inStream);
			inStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Assert.assertEquals(obj, target);
	}

	@After
	public void tearDown() {
		try {
			outStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
