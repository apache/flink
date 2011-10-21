package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class ArrayMergerTest extends EvaluableExpressionTest<ArrayAccess> {

	@Override
	protected ArrayAccess createDefaultInstance(final int index) {
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
				createArrayNode(NullNode.getInstance(), createObjectNode("fieldName", 2),
					NullNode.getInstance()), createArrayNode(createObjectNode("fieldName", 1)),
				createArrayNode(null, null, createObjectNode("fieldName", 3))), this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3)), result);
	}
}
