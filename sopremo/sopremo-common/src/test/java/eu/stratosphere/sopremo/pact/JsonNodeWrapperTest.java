package eu.stratosphere.sopremo.pact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

public class JsonNodeWrapperTest {

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
	public void shouldSerializeAndDeserialize(){
		try {
			
			ArrayNode array = new ArrayNode();
			final JsonParser parser = new JsonParser(new URL(
				SopremoTest.getResourcePath("SopremoTestPlan/test.json")));
			File file = File.createTempFile("test", "json");

			while (!parser.checkEnd()) {
				array.add(parser.readValueAsTree());
			}

			SopremoUtil.wrap(array).write(this.outStream);

			final ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(this.byteArray.toByteArray());
			final DataInputStream inStream = new DataInputStream(byteArrayIn);

			JsonNodeWrapper wrapper = new JsonNodeWrapper();
			wrapper.read(inStream);
			JsonNode target = SopremoUtil.unwrap(wrapper);

			//for watching the output
			JsonGenerator gen = new JsonGenerator(file);
			gen.writeStartArray();
			gen.writeTree(target);
			gen.writeEndArray();
			gen.close();
			
			Assert.assertEquals(array, target);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
