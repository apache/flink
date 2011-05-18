package eu.stratosphere.sopremo.function;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NumericNode;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtils;

public class FunctionRegistryTest {
	private FunctionRegistry registry;
	private EvaluationContext context;

	@Before
	public void setup() {
		context = new EvaluationContext();
		registry = context.getFunctionRegistry();
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
				JsonUtils.NODE_FACTORY.numberNode(2)), context));
	}

	@Test
	public void shouldInvokeArrayJavaFunctionForArrayNode() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(ARRAY_NODE, registry.evaluate("count", JsonUtils.NODE_FACTORY.arrayNode(), context));
	}

	@Test
	public void shouldInvokeMostSpecificVarargJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(ONE_INT_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3)), context));
		Assert.assertSame(ONE_INT_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1)), context));
	}

	@Test
	public void shouldInvokeFallbackJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_NODE, registry.evaluate("count", JsonUtils.NODE_FACTORY.objectNode(), context));
	}

	@Test
	public void shouldInvokeGenericVarargJavaFunctionsIfNoExactMatch() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_VARARG_NODE,
			registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.objectNode(),
				JsonUtils.NODE_FACTORY.objectNode(), JsonUtils.NODE_FACTORY.objectNode()), context));
	}

	@Test
	public void shouldInvokeDerivedVarargJavaFunction() {
		registry.register(JavaFunctions.class);

		Assert.assertSame(SUM_NODE,
			registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3)), context));
		Assert.assertSame(SUM_NODE,
			registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1)), context));
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoApproporiateMatchingJavaFunction() {
		registry.register(JavaFunctions.class);

		registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.textNode("3")), context);
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
