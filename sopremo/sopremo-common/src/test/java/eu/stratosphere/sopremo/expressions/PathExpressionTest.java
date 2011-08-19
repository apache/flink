package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.JsonUtil;

@Ignore
public class PathExpressionTest extends EvaluableExpressionTest<PathExpression> {

	private static JsonNode doc = getJsonNode();

	private static JsonNode getJsonNode() {
		try {
			JsonParser parser = JsonUtil.FACTORY.createJsonParser(new URL(
				getResourcePath("PathExpressionTest/test.json")));
			parser.setCodec(JsonUtil.OBJECT_MAPPER);
			return parser.readValueAsTree();
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected PathExpression createDefaultInstance(int index) {

		return new PathExpression(new ConstantExpression(IntNode.valueOf(index)));
	}

	@Test
	public void shouldReturnElementOfGivenPath() {
		final JsonNode result = new PathExpression(
			new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"), new ObjectAccess("GlossList"),
			new ObjectAccess("GlossEntry"), new ObjectAccess("GlossDef"), new ObjectAccess("GlossSeeAlso"),
			new ArrayAccess(1)).evaluate(doc, this.context);

		Assert.assertEquals(TextNode.valueOf("XML"), result);
	}
}
