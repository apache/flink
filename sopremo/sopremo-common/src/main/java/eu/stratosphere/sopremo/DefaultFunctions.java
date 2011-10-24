package eu.stratosphere.sopremo;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.aggregation.MaterializingAggregationFunction;
import eu.stratosphere.sopremo.aggregation.TransitiveAggregationFunction;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.OptimizerHints;
import eu.stratosphere.sopremo.expressions.Scope;
import eu.stratosphere.sopremo.function.MethodRegistry;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.NumericNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.util.ConcatenatingIterator;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Base built-in functions.
 * 
 * @author Arvid Heise
 */
public class DefaultFunctions implements BuiltinProvider, FunctionRegistryCallback, ConstantRegistryCallback {
	private static final NumericNode ZERO = new IntNode(0), ONE = new IntNode(1);

	public static final AggregationFunction SUM = new TransitiveAggregationFunction("sum", ZERO) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8021932798231751696L;

		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, (NumericNode) node);
		}
	};

	public static final AggregationFunction COUNT = new TransitiveAggregationFunction("count", ZERO) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4700372075569392783L;

		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, ONE);
		}
	};

	public static final AggregationFunction FIRST = new TransitiveAggregationFunction("first", NullNode.getInstance()) {
		/**
		 * 
		 */
		private static final long serialVersionUID = 273172975676646935L;

		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			return aggregate.isNull() ? node : aggregate;
		}
	};

	public static final AggregationFunction SORT = new MaterializingAggregationFunction("sort") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3035270432104235038L;

		@Override
		protected List<JsonNode> processNodes(final List<JsonNode> nodes) {
			Collections.sort(nodes);
			return nodes;
		}
	};

	public static final AggregationFunction ALL = new MaterializingAggregationFunction("all") {

		/**
		 * 
		 */
		private static final long serialVersionUID = 9079394721632933377L;
	};

	public static final AggregationFunction AVERAGE = new AggregationFunction("avg") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 483420587993286076L;

		private transient int count;

		private transient double value;

		@Override
		public void aggregate(final JsonNode node, final EvaluationContext context) {
			this.value += ((NumericNode) node).getDoubleValue();
			this.count++;
		}

		@Override
		public JsonNode getFinalAggregate() {
			if (this.count == 0)
				return DoubleNode.valueOf(Double.NaN);
			return DoubleNode.valueOf(this.value / this.count);
		}

		@Override
		public void initialize() {
			this.count = 0;
			this.value = 0;
		}
	};

	public static JsonNode format(final TextNode format, final JsonNode... params) {
		final Object[] paramsAsObjects = new Object[params.length];
		for (int index = 0; index < paramsAsObjects.length; index++)
			paramsAsObjects[index] = params[index].isTextual() ? ((TextNode) params[index]).getTextValue()
				: params[index]
					.toString();

		return TextNode.valueOf(String.format(format.getTextValue(), paramsAsObjects));
	}

	public static JsonNode substring(final TextNode input, final IntNode from, final IntNode to) {
		final String string = input.getTextValue();
		final int fromPos = resolveIndex(from.getIntValue(), string.length());
		final int toPos = resolveIndex(to.getIntValue(), string.length());

		return TextNode.valueOf(string.substring(fromPos, toPos));
	}

	// public static JsonNode extract(TextNode input, TextNode pattern, JsonNode defaultValue) {
	// Pattern compiledPattern = Pattern.compile(pattern.getTextValue());
	// Matcher matcher = compiledPattern.matcher(input.getTextValue());
	// if (!matcher.find())
	// return defaultValue;
	//
	// if (matcher.groupCount() == 1)
	// return TextNode.valueOf(matcher.group(0));
	//
	// ArrayNode result = new ArrayNode(null);
	// for (int index = 1; index <= matcher.groupCount(); index++)
	// result.add(TextNode.valueOf(matcher.group(index)));
	// return result;
	// }
	//
	// public static JsonNode extract(TextNode input, TextNode pattern) {
	// return extract(input, pattern, NullNode.getInstance());
	// }

	public static JsonNode camelCase(final TextNode input) {
		final char[] chars = input.getTextValue().toCharArray();

		boolean capitalize = true;
		for (int index = 0; index < chars.length; index++)
			if (Character.isWhitespace(chars[index]))
				capitalize = true;
			else if (capitalize) {
				chars[index] = Character.toUpperCase(chars[index]);
				capitalize = false;
			} else
				chars[index] = Character.toLowerCase(chars[index]);

		return TextNode.valueOf(new String(chars));
	}

	private static int resolveIndex(final int index, final int size) {
		if (index < 0)
			return size + index;
		return index;
	}

	/**
	 * Concatenates the textual representation of the nodes.
	 * 
	 * @param params
	 *        the nodes to concatenate
	 * @return a string node of the concatenated textual representations
	 */
	@OptimizerHints(minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode concat(final JsonNode... params) {
		final StringBuilder builder = new StringBuilder();
		for (final JsonNode jsonNode : params)
			builder.append(jsonNode.isTextual() ? ((TextNode) jsonNode).getTextValue() : jsonNode);
		return JsonUtil.OBJECT_MAPPER.valueToTree(builder);
	}

	/**
	 * Returns the number of child elements in the given node.
	 * 
	 * @param node
	 *        the children of this node are counted
	 * @return the number of child elements
	 */
	@OptimizerHints(minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode count(final JsonNode node) {
		return new IntNode(((ArrayNode) node).size());
	}

	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode sort(final JsonNode node) {
		final List<JsonNode> nodes = new ArrayList<JsonNode>();
		for (final JsonNode jsonNode : (ArrayNode) node)
			nodes.add(jsonNode);
		Collections.sort(nodes);
		final ArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(nodes);
		return arrayNode;
	}

	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode distinct(final JsonNode node) {
		final Set<JsonNode> nodes = new HashSet<JsonNode>();
		for (final JsonNode jsonNode : (ArrayNode) node)
			nodes.add(jsonNode);
		final ArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(nodes);
		return arrayNode;
	}

	public static JsonNode length(final TextNode node) {
		return IntNode.valueOf(node.getTextValue().length());
	}

	/**
	 * Repeatedly applies the {@link ArithmeticOperator#ADDITION} to the children of the given node.
	 * 
	 * @param node
	 *        the children of this node are summed up
	 * @return the sum of child elements
	 */
	@OptimizerHints(minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode sum(final JsonNode node) {
		final Iterator<JsonNode> iterator = ((ArrayNode) node).iterator();
		if (!iterator.hasNext())
			return ZERO;
		NumericNode sum = (NumericNode) iterator.next();
		for (; iterator.hasNext();)
			sum = ArithmeticExpression.ArithmeticOperator.ADDITION.evaluate(sum,
				(NumericNode) iterator.next());
		return sum;
	}

	/**
	 * Concatenates the children of the given arrays.
	 * 
	 * @param arrays
	 *        the arrays to concatenate
	 * @return the concatenated array
	 */
	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode unionAll(final JsonNode... arrays) {
		boolean hasStream = false; // , resettable = false;
		for (final JsonNode param : arrays) {
			final boolean stream = param instanceof ArrayNode;
			hasStream |= stream;
			// if (stream && ((ArrayNode) param).isResettable()) {
			// resettable = true;
			// break;
			// }
		}

		if (hasStream) {
			final Iterator<?>[] iterators = new Iterator[arrays.length];
			for (int index = 0; index < iterators.length; index++)
				iterators[index] = ((ArrayNode) arrays[index]).iterator();
			return ArrayNode.valueOf(new ConcatenatingIterator<JsonNode>(iterators)/* , resettable */);
		}

		final ArrayNode union = new ArrayNode();
		for (final JsonNode param : arrays)
			for (final JsonNode child : (ArrayNode) param)
				union.add(child);
		return union;
	}

	public static JsonNode average(NumericNode... inputs) {
		double sum = 0;

		for (NumericNode numericNode : inputs) {
			sum += ((DoubleNode) numericNode).getDoubleValue();
		}

		return DoubleNode.valueOf(sum / inputs.length);
	}

	public static JsonNode trim(TextNode input) {
		return TextNode.valueOf(input.getTextValue().trim());
	}

	public static JsonNode split(TextNode input, TextNode splitString) {
		String[] split = input.getTextValue().split(splitString.getTextValue());
		ArrayNode splitNode = new ArrayNode();
		for (String string : split)
			splitNode.add(TextNode.valueOf(string));
		return splitNode;
	}

	public static JsonNode extract(TextNode input, TextNode pattern, JsonNode defaultValue) {
		Pattern compiledPattern = Pattern.compile(pattern.getTextValue());
		Matcher matcher = compiledPattern.matcher(input.getTextValue());

		if (!matcher.find())
			return defaultValue;

		if (matcher.groupCount() == 0)
			return TextNode.valueOf(matcher.group(0));

		if (matcher.groupCount() == 1)
			return TextNode.valueOf(matcher.group(1));

		ArrayNode result = new ArrayNode();
		for (int index = 1; index <= matcher.groupCount(); index++)
			result.add(TextNode.valueOf(matcher.group(index)));
		return result;
	}

	public static JsonNode extract(TextNode input, TextNode pattern) {
		return extract(input, pattern, NullNode.getInstance());
	}

	public static JsonNode replace(TextNode input, TextNode search, TextNode replace) {
		return TextNode.valueOf(input.getTextValue().replaceAll(search.getTextValue(), replace.getTextValue()));
	}

	public static JsonNode filter(ArrayNode input, JsonNode... elementsToFilter) {
		ArrayNode output = new ArrayNode();
		HashSet<JsonNode> filterSet = new HashSet<JsonNode>(Arrays.asList(elementsToFilter));
		for (int index = 0; index < input.size(); index++)
			if (!filterSet.contains(input.get(index)))
				output.add(input.get(index));
		return output;
	}

	@Override
	public void registerFunctions(MethodRegistry registry) {
		List<Method> methods = ReflectUtil.getMethods(String.class, null, Modifier.PUBLIC, ~Modifier.STATIC);
		for (Method method : methods)
			try {
				registry.register(method);
			} catch (Exception e) {
				// System.out.println("Could not register " + method);
			}
	}
	
	@Override
	public void registerConstants(EvaluationContext context) {
		context.setBinding("pi", new ConstantExpression(Math.PI));
		context.setBinding("e", new ConstantExpression(Math.E));
	}

	//
	// public static JsonNode group(ArrayNode array, TextNode elementName) {
	// return extract(input, pattern, NullNode.getInstance());
	// }
	//
	// public static JsonNode aggregate(ArrayNode array, EvaluationExpression groupingExpression, EvaluationExpression
	// aggregation) {
	// final List<CompactArrayNode> nodes = new ArrayList<CompactArrayNode>();
	// for (final JsonNode jsonNode : array)
	// nodes.add(JsonUtil.asArray(groupingExpression.evaluate(jsonNode, null)));
	// Collections.sort(nodes, JsonNodeComparator.INSTANCE);
	// final ArrayNode arrayNode = new ArrayNode(null);
	// arrayNode.addAll(nodes);
	// return arrayNode;
	// }

	public static final AggregationFunction MIN = new TransitiveAggregationFunction("min", NullNode.getInstance()) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8124401653435722884L;

		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			if (aggregate.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(node, aggregate))
				return node;
			return aggregate;
		}
	};

	public static final AggregationFunction MAX = new TransitiveAggregationFunction("max", NullNode.getInstance()) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -1735264603829085865L;

		@Override
		protected JsonNode aggregate(final JsonNode aggregate, final JsonNode node, final EvaluationContext context) {
			if (aggregate.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(aggregate, node))
				return node;
			return aggregate;
		}
	};
}
