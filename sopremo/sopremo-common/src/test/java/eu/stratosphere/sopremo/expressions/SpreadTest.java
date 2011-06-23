package eu.stratosphere.sopremo.expressions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;

public class SpreadTest extends EvaluableExpressionTest<ArrayMap> {
	@Override
	protected ArrayMap createDefaultInstance(int index) {
		return new ArrayMap(new ObjectAccess(String.valueOf(index)));
	}

	@Test
	public void shouldAccessFieldOfArray() {
		JsonNode result = new ArrayMap(new ObjectAccess("fieldName")).evaluate(
			createArrayNode(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createArrayNode(1, 2, 3), result);
	}

	@Test
	public void shouldAccessFieldOfStreamArray() {
		JsonNode result = new ArrayMap(new ObjectAccess("fieldName")).evaluate(
			createStreamArray(createObjectNode("fieldName", 1), createObjectNode("fieldName", 2),
				createObjectNode("fieldName", 3)),
			context);
		Assert.assertEquals(createStreamArray(1, 2, 3), result);
	}

}
