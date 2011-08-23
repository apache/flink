package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ObjectCreationTest extends EvaluableExpressionTest<ObjectCreation> {

	@Override
	protected ObjectCreation createDefaultInstance(int index) {
		return new ObjectCreation(new ObjectCreation.Mapping(String.valueOf(index), new ConstantExpression(
			IntNode.valueOf(index))));
	}

	@Test
	public void shouldCreateObjectAsIntended() {
		final JsonNode result = new ObjectCreation(new ObjectCreation.Mapping("name", new ConstantExpression(
			TextNode.valueOf("testperson"))), new ObjectCreation.Mapping("age", new ConstantExpression(
			IntNode.valueOf(30)))).evaluate(IntNode.valueOf(0), this.context);

		Assert.assertEquals(createObjectNode("name", "testperson", "age", 30), result);
	}

	@Test
	public void shouldReturnSpecifiedMapping() {
		final ObjectCreation.Mapping mapping = new ObjectCreation(new ObjectCreation.Mapping("name",
			new ConstantExpression(
				TextNode.valueOf("testperson"))), new ObjectCreation.Mapping("age", new ConstantExpression(
			IntNode.valueOf(30)))).getMapping(1);

		Assert.assertEquals(new ObjectCreation.Mapping("age", new ConstantExpression(IntNode.valueOf(30))), mapping);
	}

	@Test
	public void shouldAddMappings() {

		ObjectCreation object = new ObjectCreation(new ObjectCreation.Mapping("name", new ConstantExpression(
			TextNode.valueOf("testperson"))), new ObjectCreation.Mapping("age", new ConstantExpression(
			IntNode.valueOf(30))));

		object.addMapping("birthday", new ConstantExpression(TextNode.valueOf("01.01.2000")));

		final JsonNode result = object.evaluate(IntNode.valueOf(0), this.context);

		Assert.assertEquals(createObjectNode("name", "testperson", "age", 30, "birthday", "01.01.2000"), result);
	}

	@Test
	public void shouldEvaluateMappings() {
		final ObjectCreation.Mapping mapping = new ObjectCreation.Mapping("testname", new InputSelection(0));
		ObjectNode result = createObjectNode("fieldname", "test");

		mapping.evaluate(result, (JsonNode) createArrayNode("1", "2"), this.context);
		mapping.evaluate(result, (JsonNode) createArrayNode("3", "4"), this.context);

		Assert.assertEquals(createObjectNode("fieldname", "test", "testname", "3"), result);
	}
}
