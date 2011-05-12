package eu.stratosphere.sopremo;

import java.util.Collection;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.ValueNode;
import org.junit.Before;
import org.junit.Test;

public class FunctionRegistryTest {
	private FunctionRegistry registry;

	@Before
	public void setup() {
		registry = new FunctionRegistry();
	}

	@Test
	public void shouldAddJavaFunctions() {
		registry.register(JavaFunctions.class);

		Assert.assertEquals("should have been 2 functions", 2, registry.getRegisteredFunctions().size());
		for (Function function : registry.getRegisteredFunctions().values())
			Assert.assertEquals("should have been a java function", JavaFunction.class, function.getClass());

		Assert.assertEquals("should have been 5 count signatures", 5, ((JavaFunction) registry.getFunction("count"))
			.getSignatures().size());
		Assert.assertEquals("should have been 1 sum signatures", 1, ((JavaFunction) registry.getFunction("sum"))
			.getSignatures().size());
	}

	@Test
	public void shouldInvokeExactMatchingJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(TWO_INT_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2))));
	}

	@Test
	public void shouldInvokeArrayJavaFunctionForArrayNode() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(ARRAY_NODE, registry.evaluate("count", JsonUtils.NODE_FACTORY.arrayNode()));
	}

	@Test
	public void shouldInvokeMostSpecificVarargJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(ONE_INT_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3))));
		Assert.assertSame(ONE_INT_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1))));
	}

	@Test
	public void shouldInvokeFallbackJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_NODE, registry.evaluate("count", JsonUtils.NODE_FACTORY.objectNode()));
	}

	@Test
	public void shouldInvokeGenericVarargJavaFunctionsIfNoExactMatch() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.objectNode(),
				JsonUtils.NODE_FACTORY.objectNode(), JsonUtils.NODE_FACTORY.objectNode())));
	}

	@Test
	public void shouldInvokeDerivedVarargJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(SUM_NODE,
			registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3))));
		Assert.assertSame(SUM_NODE,
			registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1))));
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoApproporiateMatchingJavaFunction() {
		registry.register(JavaFunctions.class);

		registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.textNode("3")));
	}

	private static final JsonNode GENERIC_VARARG_NODE = JsonUtils.NODE_FACTORY.textNode("var");

	private static final JsonNode GENERIC_NODE = JsonUtils.NODE_FACTORY.textNode("generic");

	private static final JsonNode ARRAY_NODE = JsonUtils.NODE_FACTORY.textNode("array");

	private static final JsonNode TWO_INT_NODE = JsonUtils.NODE_FACTORY.textNode("2 int");

	private static final JsonNode ONE_INT_VARARG_NODE = JsonUtils.NODE_FACTORY.textNode("1 int + var");

	private static final JsonNode SUM_NODE = JsonUtils.NODE_FACTORY.textNode("sum");

	public static class JavaFunctions {

		public static JsonNode count(JsonNode node) {
			return GENERIC_NODE;
		}

		public static JsonNode count(JsonNode... node) {
			return GENERIC_VARARG_NODE;
		}

		public static JsonNode count(ArrayNode node) {
			return ARRAY_NODE;
		}

		public static JsonNode count(IntNode node, IntNode node2) {
			return TWO_INT_NODE;
		}

		public static JsonNode count(IntNode node, IntNode... nodes) {
			return ONE_INT_VARARG_NODE;
		}

		public static JsonNode sum(NumericNode... nodes) {
			return SUM_NODE;
		}
	}
}
