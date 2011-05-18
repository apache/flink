package eu.stratosphere.sopremo.function;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;

public class BuiltinFunctions {
	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	private static final JsonNode ZERO = OBJECT_MAPPER.valueToTree(0);

	public static JsonNode count(JsonNode params) {
		int count = 0;
		Iterator<JsonNode> iterator = params.iterator();
		for (; iterator.hasNext(); count++)
			iterator.next();
		return OBJECT_MAPPER.valueToTree(count);
	}

	public static JsonNode sum(JsonNode params) {
		Iterator<JsonNode> iterator = params.iterator();
		if (!iterator.hasNext())
			return ZERO;
		JsonNode sum = iterator.next();
		for (; iterator.hasNext();)
			sum = Arithmetic.ArithmeticOperator.PLUS.evaluate((NumericNode) sum, (NumericNode) iterator.next());
		return OBJECT_MAPPER.valueToTree(sum);
	}
}
