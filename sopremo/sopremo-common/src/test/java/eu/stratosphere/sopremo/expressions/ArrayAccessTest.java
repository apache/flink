package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ArrayAccessTest extends EvaluableExpressionTest<ArrayAccess> {
	@Override
	protected ArrayAccess createDefaultInstance(final int index) {
		return new ArrayAccess(index);
	}

	@Test
	public void shouldAccessAllElements() {
		final IJsonNode result = new ArrayAccess().evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessOneElement() {
		final IJsonNode result = new ArrayAccess(1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			null, this.context);
		Assert.assertEquals(createObjectNode("fieldName", 2), result);
	}

	@Test
	public void shouldAccessOneElementWithNegativeIndex() {
		final IJsonNode result = new ArrayAccess(-1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			null, this.context);
		Assert.assertEquals(createObjectNode("fieldName", 3), result);
	}

	@Test
	public void shouldAccessRangeOfElement() {
		final IJsonNode result = new ArrayAccess(1, 3).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeEndIndex() {
		final IJsonNode result = new ArrayAccess(1, -1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeStartAndEndIndex() {
		final IJsonNode result = new ArrayAccess(-3, -1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeStartIndex() {
		final IJsonNode result = new ArrayAccess(-3, 4).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessReversedRangeOfElements() {
		final IJsonNode result = new ArrayAccess(3, 1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			null, this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 4), createObjectNode("fieldName", 3),
			createObjectNode("fieldName", 2)), result);
	}

	@Test
	public void shouldReuseTargetIfWholeNodeIsAccessed() {
		IJsonNode target = new ArrayNode();

		final IJsonNode result = new ArrayAccess().evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			target, this.context);

		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
		Assert.assertSame(target, result);
	}

	@Test
	public void shouldNotReuseTargetIfWrongType() {
		IJsonNode target = new ObjectNode();

		final IJsonNode result = new ArrayAccess().evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2)), target, this.context);

		Assert
			.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2)), result);
		Assert.assertNotSame(target, result);
	}

	@Test
	public void shouldReuseTargetIfRangeIsAccessed() {
		IJsonNode target = new ArrayNode();

		final IJsonNode result = new ArrayAccess(2, 4).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			target, this.context);

		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
		Assert.assertSame(target, result);
	}
}
