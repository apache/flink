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

import eu.stratosphere.sopremo.io.JsonParser.PrimitiveParser;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BigIntegerNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.DecimalNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.LongNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

@RunWith(Parameterized.class)
public class JsonParserTest {

	private final String value;

	private final JsonNode expectedResult;

	public JsonParserTest(final String value, final JsonNode expectedResult) {
		this.value = value;
		this.expectedResult = expectedResult;
	}

	// @Test
	// public void shouldParse() throws IOException{
	// JsonNode result = new JsonParser(this.value).readValueAsTree();
	// Assert.assertEquals(this.expectedResult, result);
	// }

	@Test
	public void shouldParseArrays() throws IOException {
		JsonNode result = new JsonParser(this.value).readValueAsTree();
		Assert.assertEquals(this.expectedResult, result);
	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ "42", IntNode.valueOf(42) },
			{ "null", NullNode.getInstance() },
			{ "true", BooleanNode.TRUE },
			{ "false", BooleanNode.FALSE },
			{ "42.42", DecimalNode.valueOf(BigDecimal.valueOf(42.42)) },
			{ String.valueOf(Long.valueOf(Integer.MAX_VALUE) + 1),
				LongNode.valueOf(Long.valueOf(Integer.MAX_VALUE) + 1) },
			{ String.valueOf(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
				BigIntegerNode.valueOf(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)) },
			{ "42shadh34634", TextNode.valueOf("42shadh34634") },
			{ "[42, 23]", new ArrayNode().add(IntNode.valueOf(42)).add(IntNode.valueOf(23)) },
			{
				"[42, [23, [[24, 55], 12, 17]]]",
				new ArrayNode().add(IntNode.valueOf(42)).add(
					new ArrayNode().add(IntNode.valueOf(23)).add(
						new ArrayNode().add(new ArrayNode().add(IntNode.valueOf(24)).add(IntNode.valueOf(55)))
							.add(IntNode.valueOf(12)).add(IntNode.valueOf(17)))) },
			{ "\"Test\"", TextNode.valueOf("Test")},
			{ "\"Test\\\"Test\"", TextNode.valueOf("Test\\\"Test")},
			{
				"[42, [23, [[24, \"Test\"], 12, \"17\"]]]",
				new ArrayNode().add(IntNode.valueOf(42)).add(
					new ArrayNode().add(IntNode.valueOf(23)).add(
						new ArrayNode().add(new ArrayNode().add(IntNode.valueOf(24)).add(TextNode.valueOf("Test")))
							.add(IntNode.valueOf(12)).add(TextNode.valueOf("17")))) }

		});
	}
}
