package eu.stratosphere.sopremo.function;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.operator.StreamArrayNode;
import eu.stratosphere.util.AbstractIterator;
import eu.stratosphere.util.ConcatenatingIterator;

public class BuiltinFunctions {
	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	private static final JsonNode ZERO = OBJECT_MAPPER.valueToTree(0);

	private static final JsonNode EMPTY_STRING = OBJECT_MAPPER.valueToTree("");

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
		return sum;
	}

	public static JsonNode concat(JsonNode[] params) {
		StringBuilder builder = new StringBuilder();
		for (JsonNode jsonNode : params) 
			builder.append(jsonNode.isTextual() ? jsonNode.getTextValue() : jsonNode);
		return OBJECT_MAPPER.valueToTree(builder);
	}

	public static JsonNode union(JsonNode[] params) {
		boolean hasStream = false;
		for (JsonNode param : params) {
			if (!param.isArray())
				throw new EvaluationException("Can only union arrays");
			hasStream |= param instanceof StreamArrayNode;
		}

		if (hasStream) {
			Iterator<?>[] iterators = new Iterator[params.length];
			for (int index = 0; index < iterators.length; index++)
				iterators[index] = params[index].iterator();
			return new StreamArrayNode(new ConcatenatingIterator<JsonNode>(iterators));
		}

		ArrayNode union = NODE_FACTORY.arrayNode();
		for (JsonNode param : params)
			union.addAll((ArrayNode) param);
		return union;
	}
}
