package eu.stratosphere.sopremo.expressions;
import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class InputSelectionTest extends EvaluableExpressionTest<InputSelection> {

	@Override
	protected InputSelection createDefaultInstance(int index) {
		return new InputSelection(index);
	}

	@Test
	public void shouldSelectCorrectInput() {
		final JsonNode result = new InputSelection(1).evaluate(createArrayNode(IntNode.valueOf(0), IntNode.valueOf(1)),
			this.context);

		Assert.assertEquals(IntNode.valueOf(1), result);
	}
}
