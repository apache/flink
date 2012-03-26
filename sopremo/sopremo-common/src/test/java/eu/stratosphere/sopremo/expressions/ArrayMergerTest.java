package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ArrayMergerTest extends EvaluableExpressionTest<ArrayAccess> {

	@Override
	protected ArrayAccess createDefaultInstance(final int index) {
		return new ArrayAccess(index);
	}

	@Test
	public void shouldMergeOneArray() {
		final IJsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5))),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldMergeEmptyArray() {
		final IJsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)), createArrayNode()),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldFillNullValuesWithValuesFromOtherArrays() {
		final IJsonNode result = new ArrayMerger().evaluate(
			createArrayNode(createArrayNode(null, createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)), createArrayNode(createObjectNode("fieldName", 1))),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldFillNullNodesWithValuesFromOtherArrays() {
		final IJsonNode result = new ArrayMerger().evaluate(
			createArrayNode(
				createArrayNode(NullNode.getInstance(), createObjectNode("fieldName", 2),
					NullNode.getInstance()), createArrayNode(createObjectNode("fieldName", 1)),
				createArrayNode(null, null, createObjectNode("fieldName", 3))), null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3)), result);
	}

	@Test
	public void shouldReuseTarget() {
		IJsonNode target = new ArrayNode();

		IArrayNode firstArray = createArrayNode(null, IntNode.valueOf(2), IntNode.valueOf(3));
		IArrayNode secondArray = createArrayNode(IntNode.valueOf(1));

		IJsonNode result = new ArrayMerger().evaluate(createArrayNode(firstArray, secondArray), target, this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3)), result);
		Assert.assertSame(target, result);
	}

	@Test
	public void shouldNotReuseTargetIfWrongType() {
		IJsonNode target = new ObjectNode();

		IArrayNode firstArray = createArrayNode(null, IntNode.valueOf(2), IntNode.valueOf(3));
		IArrayNode secondArray = createArrayNode(IntNode.valueOf(1));

		IJsonNode result = new ArrayMerger().evaluate(createArrayNode(firstArray, secondArray), target, this.context);

		Assert.assertEquals(createArrayNode(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3)), result);
		Assert.assertNotSame(target, result);
	}
}
