package eu.stratosphere.usecase.cleansing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.aggregation.TransitiveAggregationFunction;
import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class CleansFunctions extends BuiltinFunctions {
	public static JsonNode length(final TextNode node) {
		return IntNode.valueOf(node.getTextValue().length());
	}
	
	private static Map<String, Class<? extends JsonNode>> typeNameToType = new HashMap<String, Class<?extends JsonNode>>();
	
	static {
		typeNameToType.put("string", TextNode.class);
		typeNameToType.put("text", TextNode.class);
		typeNameToType.put("int", IntNode.class);
	}

	public static JsonNode coerce(JsonNode input, TextNode type) {
		return  TypeCoercer.INSTANCE.coerce(input, typeNameToType.get(type));
	}
	
	public static JsonNode extract(TextNode input, TextNode pattern, TextNode expectedType) {
		Pattern compiledPattern = Pattern.compile(pattern.getTextValue());
		Matcher matcher = compiledPattern.matcher(input.getTextValue());

		if (!matcher.find())
			return NullNode.getInstance();

		if (matcher.groupCount() ==0)
			return TextNode.valueOf(matcher.group(0));
			
		if (matcher.groupCount() == 1)
			return TextNode.valueOf(matcher.group(1));

		ArrayNode result = new ArrayNode(null);
		for (int index = 1; index <= matcher.groupCount(); index++)
			result.add(TextNode.valueOf(matcher.group(index)));
		return result;
	}

	public static JsonNode extract(TextNode input, TextNode pattern) {
		return extract(input, pattern, TextNode.valueOf("string"));
	}
	
//
//	public static JsonNode group(ArrayNode array, TextNode elementName) {
//		return extract(input, pattern, NullNode.getInstance());
//	}
//	
//	public static JsonNode aggregate(ArrayNode array, EvaluationExpression groupingExpression, EvaluationExpression aggregation) {
//		final List<CompactArrayNode> nodes = new ArrayList<CompactArrayNode>();
//		for (final JsonNode jsonNode : array)
//			nodes.add(JsonUtil.asArray(groupingExpression.evaluate(jsonNode, null)));
//		Collections.sort(nodes, JsonNodeComparator.INSTANCE);
//		final ArrayNode arrayNode = new ArrayNode(null);
//		arrayNode.addAll(nodes);
//		return arrayNode;
//	}

	public static final AggregationFunction MIN = new TransitiveAggregationFunction("min", NullNode.getInstance()) {
		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			if (aggregate.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(node, aggregate))
				return node;
			return aggregate;
		}
	};

	public static final AggregationFunction MAX = new TransitiveAggregationFunction("max", NullNode.getInstance()) {
		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			if (aggregate.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(aggregate, node))
				return node;
			return aggregate;
		}
	};
}
