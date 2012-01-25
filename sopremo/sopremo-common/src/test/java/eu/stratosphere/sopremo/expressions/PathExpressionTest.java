package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class PathExpressionTest extends EvaluableExpressionTest<PathExpression> {

	private static JsonNode doc = getJsonNode();

	private static JsonNode getJsonNode() {
		try {
			final JsonParser parser = new JsonParser(new URL(
				getResourcePath("PathExpressionTest/test.json")));
			// parser.setCodec(JsonUtil.OBJECT_MAPPER);
			return parser.readValueAsTree();
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected PathExpression createDefaultInstance(final int index) {

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

	@Test
	public void shouldAddNewExpression() {
		final PathExpression expr = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry"));
		expr.add(new ObjectAccess("ID"));
		final JsonNode result = expr.evaluate(doc, this.context);

		Assert.assertEquals(TextNode.valueOf("SGML"), result);
	}

	@Test
	public void shouldRecognizeTruePrefix() {
		final PathExpression prefix = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"));
		final boolean result = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry")).isPrefix(prefix);

		Assert.assertTrue(result);
	}

	@Test
	public void shouldRejectWrongPrefix() {
		final PathExpression prefix = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("title"));
		final boolean result = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry")).isPrefix(prefix);

		Assert.assertFalse(result);
	}

	@Test
	public void shouldRejectPrefixWithWrongLength() {
		final PathExpression prefix = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry"));
		final boolean result = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"))
			.isPrefix(prefix);

		Assert.assertFalse(result);
	}

	@Test
	public void shouldReplaceRightFragment() {
		final PathExpression expr = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry"), new ObjectAccess("ID"));
		expr.replace(new ObjectAccess("ID"), new ObjectAccess("GlossSee"));

		final JsonNode result = expr.evaluate(doc, this.context);

		Assert.assertEquals(TextNode.valueOf("markup"), result);
	}

	@Test
	public void shouldReplaceWholePath() {
		final PathExpression expr = new PathExpression(new ObjectAccess("glossary"), new ObjectAccess("GlossDiv"),
			new ObjectAccess("GlossList"), new ObjectAccess("GlossEntry"), new ObjectAccess("ID"));
		expr.replace(new PathExpression(new ObjectAccess("GlossDiv"), new ObjectAccess("GlossList"), new ObjectAccess(
			"GlossEntry"), new ObjectAccess("ID")), new ObjectAccess("title"));

		final JsonNode result = expr.evaluate(doc, this.context);

		Assert.assertEquals(TextNode.valueOf("example glossary"), result);
	}

	@Test
	public void shouldFindRightFragment() {
		final EvaluationExpression result = new PathExpression(new ObjectAccess("glossary"),
			new ObjectAccess("GlossDiv"), new ObjectAccess("GlossList")).getFragment(1);

		Assert.assertEquals(new ObjectAccess("GlossDiv"), result);
	}

	@Override
	protected void initVerifier(final EqualsVerifier<PathExpression> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(LinkedList.class,
			new LinkedList<Object>(),
			new LinkedList<Object>(Arrays.asList(new ObjectAccess("field"))));
	}
}