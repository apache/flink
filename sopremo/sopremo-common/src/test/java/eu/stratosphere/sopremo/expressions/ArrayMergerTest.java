package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import eu.stratosphere.sopremo.JsonUtil;

public class ArrayMergerTest extends EvaluableExpressionTest<ArrayAccess> {

	@Override
	protected ArrayAccess createDefaultInstance(int index) {
		return new ArrayAccess(index);
	}

	@Test
	public void shouldMergeOneArray() {
		final JsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5))),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldMergeEmptyArray() {
		final JsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)), createArrayNode()),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldFillNullValuesWithValuesFromOtherArrays() {
		final JsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(null, createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)), createArrayNode(createObjectNode("fieldName", 1))),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldFillNullNodesWithValuesFromOtherArrays() {
		final JsonNode result = new ArrayMerger().evaluate(
			createArrayNode(
				createArrayNode(JsonUtil.NODE_FACTORY.nullNode(), createObjectNode("fieldName", 2),
					JsonUtil.NODE_FACTORY.nullNode()), createArrayNode(createObjectNode("fieldName", 1)),
				createArrayNode(null, null, createObjectNode("fieldName", 3))), this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3)), result);
	}
}
