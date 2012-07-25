package eu.stratosphere.sopremo.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@RunWith(Parameterized.class)
public class JsonParserTest {

	private final String value;

	private final IJsonNode expectedResult;

	private final int steps;

	public JsonParserTest(final String value, final IJsonNode expectedResult, final int steps) {
		this.value = value;
		this.expectedResult = expectedResult;
		this.steps = steps;
	}

	// @Test
	// public void shouldParse() throws IOException{
	// JsonNode<?> result = new JsonParser(this.value).readValueAsTree();
	// Assert.assertEquals(this.expectedResult, result);
	// }

	@Test
	public void shouldParseArrays() throws IOException {
		final JsonParser parser = new JsonParser(this.value);
		IJsonNode result = null;
		for (int i = 0; i < this.steps; i++)
			result = parser.readValueAsTree();
		Assert.assertEquals(this.expectedResult, result);
		Assert.assertEquals(true, parser.checkEnd());
	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ " [42] ", IntNode.valueOf(42), 1 },
			{ "42", IntNode.valueOf(42), 1 },
			{ "[null]", NullNode.getInstance(), 1 },
			{ "null", NullNode.getInstance(), 1 },
			{ "[null,null]", NullNode.getInstance(), 2 },
			{ "[true]", BooleanNode.TRUE, 1 },
			{ "[false]", BooleanNode.FALSE, 1 },
			{ "[42.42]", DecimalNode.valueOf(BigDecimal.valueOf(42.42)), 1 },
			{ String.valueOf("[" + String.valueOf(Long.valueOf(Integer.MAX_VALUE) + 1) + "]"),
				LongNode.valueOf(Long.valueOf(Integer.MAX_VALUE) + 1), 1 },
			{ String.valueOf("[" + BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE) + "]"),
				BigIntegerNode.valueOf(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)), 1 },
			{ "[42shadh34634]", TextNode.valueOf("42shadh34634"), 1 },
			{ "[42, 23]", IntNode.valueOf(23), 2 },
			{
				"[42, [23, [[24, 55], 12, 17]]]",
				new ArrayNode().add(IntNode.valueOf(23)).add(
					new ArrayNode().add(new ArrayNode().add(IntNode.valueOf(24)).add(IntNode.valueOf(55)))
						.add(IntNode.valueOf(12)).add(IntNode.valueOf(17))), 2 },
			{ "[\"Test\"]", TextNode.valueOf("Test"), 1 },
			{ "[\"Test\\\"Test\"]", TextNode.valueOf("Test\\\"Test"), 1 },
			{
				"[42, [23, [[24, \"Test\"], 12, \"17\"]]]",

				new ArrayNode().add(IntNode.valueOf(23)).add(
					new ArrayNode().add(new ArrayNode().add(IntNode.valueOf(24)).add(TextNode.valueOf("Test"))).add(
						IntNode.valueOf(12)).add(TextNode.valueOf("17"))), 2 },
			{ "[{\"key1\" : 42}, 42]", IntNode.valueOf(42), 2 },
			{ "{\"key1\" : 42}", new ObjectNode().put("key1", IntNode.valueOf(42)), 1 },
			{ "{\"key1\" : null}", new ObjectNode().put("key1", NullNode.getInstance()), 1 },
			{
				"[{\"key1\" : [1,3,\"Hello\"], \"key2\": {\"key3\": 23}}]",
				new ObjectNode().put("key1",
					new ArrayNode().add(IntNode.valueOf(1)).add(IntNode.valueOf(3)).add(TextNode.valueOf("Hello")))
					.put(
						"key2", new ObjectNode().put("key3", IntNode.valueOf(23))), 1 },
			{ "[1 ,2 ,3, 4 , null]", NullNode.getInstance(), 5 }

		});
	}
}
