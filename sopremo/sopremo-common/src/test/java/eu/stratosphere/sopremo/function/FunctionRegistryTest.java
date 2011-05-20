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
		this.context = new EvaluationContext();
		this.registry = this.context.getFunctionRegistry();
	}

	@Test
	public void shouldAddJavaFunctions() {
		this.registry.register(JavaFunctions.class);

		Assert.assertEquals("should have been 2 functions", 2, this.registry.getRegisteredFunctions().size());
		for (Function function : this.registry.getRegisteredFunctions().values())
			Assert.assertEquals("should have been a java function", JavaFunction.class, function.getClass());

		Assert.assertEquals("should have been 5 count signatures", 5, ((JavaFunction) this.registry.getFunction("count"))
			.getSignatures().size());
		Assert.assertEquals("should have been 1 sum signatures", 1, ((JavaFunction) this.registry.getFunction("sum"))
			.getSignatures().size());
	}

	@Test
	public void shouldInvokeExactMatchingJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(TWO_INT_NODE,
			this.registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2)), this.context));
	}

	@Test
	public void shouldInvokeArrayJavaFunctionForArrayNode() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(ARRAY_NODE, this.registry.evaluate("count", JsonUtils.NODE_FACTORY.arrayNode(), this.context));
	}

	@Test
	public void shouldInvokeMostSpecificVarargJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(ONE_INT_VARARG_NODE,
			this.registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3)), this.context));
		Assert.assertSame(ONE_INT_VARARG_NODE,
			this.registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1)), this.context));
	}

	@Test
	public void shouldInvokeFallbackJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_NODE, this.registry.evaluate("count", JsonUtils.NODE_FACTORY.objectNode(), this.context));
	}

	@Test
	public void shouldInvokeGenericVarargJavaFunctionsIfNoExactMatch() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(GENERIC_VARARG_NODE,
			this.registry.evaluate("count", JsonUtils.asArray(JsonUtils.NODE_FACTORY.objectNode(),
				JsonUtils.NODE_FACTORY.objectNode(), JsonUtils.NODE_FACTORY.objectNode()), this.context));
	}

	@Test
	public void shouldInvokeDerivedVarargJavaFunction() {
		this.registry.register(JavaFunctions.class);

		Assert.assertSame(SUM_NODE,
			this.registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.numberNode(3)), this.context));
		Assert.assertSame(SUM_NODE,
			this.registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1)), this.context));
	}

	@Test(expected = EvaluationException.class)
	public void shouldFailIfNoApproporiateMatchingJavaFunction() {
		this.registry.register(JavaFunctions.class);

		this.registry.evaluate("sum", JsonUtils.asArray(JsonUtils.NODE_FACTORY.numberNode(1),
				JsonUtils.NODE_FACTORY.numberNode(2), JsonUtils.NODE_FACTORY.textNode("3")), this.context);
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
