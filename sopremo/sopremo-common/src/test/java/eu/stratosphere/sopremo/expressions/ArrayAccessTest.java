package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

public class ArrayAccessTest extends EvaluableExpressionTest<ArrayAccess> {
	@Override
	protected ArrayAccess createDefaultInstance(final int index) {
		return new ArrayAccess(index);
	}

	@Test
	public void shouldAccessAllElements() {
		final JsonNode result = new ArrayAccess().evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessOneElement() {
		final JsonNode result = new ArrayAccess(1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createObjectNode("fieldName", 2), result);
	}

	@Test
	public void shouldAccessOneElementWithNegativeIndex() {
		final JsonNode result = new ArrayAccess(-1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			this.context);
		Assert.assertEquals(createObjectNode("fieldName", 3), result);
	}

	@Test
	public void shouldAccessRangeOfElement() {
		final JsonNode result = new ArrayAccess(1, 3).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeEndIndex() {
		final JsonNode result = new ArrayAccess(1, -1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 2),
			createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeStartAndEndIndex() {
		final JsonNode result = new ArrayAccess(-3, -1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessRangeOfElementWithNegativeStartIndex() {
		final JsonNode result = new ArrayAccess(-3, 4).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
			createObjectNode("fieldName", 5)), result);
	}

	@Test
	public void shouldAccessReversedRangeOfElements() {
		final JsonNode result = new ArrayAccess(3, 1).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3), createObjectNode("fieldName", 4),
				createObjectNode("fieldName", 5)),
			this.context);
		Assert.assertEquals(createArrayNode(createObjectNode("fieldName", 4), createObjectNode("fieldName", 3),
			createObjectNode("fieldName", 2)), result);
	}
}
