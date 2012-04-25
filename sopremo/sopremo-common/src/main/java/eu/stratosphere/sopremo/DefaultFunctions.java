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
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
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
		protected IJsonNode aggregate(final IJsonNode aggregate, final IJsonNode node, final EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, (NumericNode) node);
		}
	};

	public static final AggregationFunction COUNT = new TransitiveAggregationFunction("count", ZERO) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4700372075569392783L;

		@Override
		protected IJsonNode aggregate(final IJsonNode aggregate, final IJsonNode node, final EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, ONE);
		}
	};

	public static final AggregationFunction FIRST = new TransitiveAggregationFunction("first", NullNode.getInstance()) {
		/**
		 * 
		 */
		private static final long serialVersionUID = 273172975676646935L;

		@Override
		protected IJsonNode aggregate(final IJsonNode aggregate, final IJsonNode node, final EvaluationContext context) {
			return aggregate.isNull() ? node : aggregate;
		}
	};

	public static final AggregationFunction SORT = new MaterializingAggregationFunction("sort") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3035270432104235038L;

		@Override
		protected List<IJsonNode> processNodes(final List<IJsonNode> nodes) {
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
		public void aggregate(final IJsonNode node, final EvaluationContext context) {
			this.value += ((INumericNode) node).getDoubleValue();
			this.count++;
		}

		@Override
		public IJsonNode getFinalAggregate() {
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

	public static IJsonNode format(final TextNode format, final IJsonNode... params) {
		final Object[] paramsAsObjects = new Object[params.length];
		for (int index = 0; index < paramsAsObjects.length; index++)
			paramsAsObjects[index] = params[index].isTextual() ? ((TextNode) params[index]).getTextValue()
				: params[index]
					.toString();

		return TextNode.valueOf(String.format(format.getTextValue(), paramsAsObjects));
	}

	public static IJsonNode substring(final TextNode input, final IntNode from, final IntNode to) {
		final String string = input.getTextValue();
		final int fromPos = resolveIndex(from.getIntValue(), string.length());
		final int toPos = resolveIndex(to.getIntValue(), string.length());

		return TextNode.valueOf(string.substring(fromPos, toPos));
	}

	// public static IJsonNode extract(TextNode input, TextNode pattern, IJsonNode defaultValue) {
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
	// public static IJsonNode extract(TextNode input, TextNode pattern) {
	// return extract(input, pattern, NullNode.getInstance());
	// }

	public static IJsonNode camelCase(final TextNode input) {
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
	public static IJsonNode concat(final IJsonNode... params) {
		final StringBuilder builder = new StringBuilder();
		for (final IJsonNode jsonNode : params)
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
	public static IJsonNode count(final IJsonNode node) {
		return new IntNode(((IArrayNode) node).size());
	}

	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static IJsonNode sort(final IJsonNode node) {
		final List<IJsonNode> nodes = new ArrayList<IJsonNode>();
		for (final IJsonNode jsonNode : (IArrayNode) node)
			nodes.add(jsonNode);
		Collections.sort(nodes);
		final ArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(nodes);
		return arrayNode;
	}

	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static IJsonNode distinct(final IJsonNode node) {
		final Set<IJsonNode> nodes = new HashSet<IJsonNode>();
		for (final IJsonNode jsonNode : (IArrayNode) node)
			nodes.add(jsonNode);
		final ArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(nodes);
		return arrayNode;
	}

	public static IJsonNode length(final TextNode node) {
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
	public static IJsonNode sum(final IJsonNode node) {
		final Iterator<IJsonNode> iterator = ((ArrayNode) node).iterator();
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
	public static IJsonNode unionAll(final IJsonNode... arrays) {
		boolean hasStream = false; // , resettable = false;
		for (final IJsonNode param : arrays) {
			final boolean stream = param instanceof ArrayNode;
			hasStream |= stream;
			// if (stream && ((ArrayNode) param).isResettable()) {
			// resettable = true;
			// break;
			// }
		}

		if (hasStream) {
			final List<Iterator<IJsonNode>> iterators = new ArrayList<Iterator<IJsonNode>>(arrays.length);
			for (int index = 0; index < arrays.length; index++)
				iterators.add(((ArrayNode) arrays[index]).iterator());
			return ArrayNode.valueOf(new ConcatenatingIterator<IJsonNode>(iterators)/* , resettable */);
		}

		final ArrayNode union = new ArrayNode();
		for (final IJsonNode param : arrays)
			for (final IJsonNode child : (IArrayNode) param)
				union.add(child);
		return union;
	}

	/**
	 * Adds the specified node to the array at the given index
	 * 
	 * @param array
	 *        the array which should be extended
	 * @param node
	 *        the node to add
	 * @param index
	 *        the position of the insert
	 * @return array with the added node
	 */
	public static IJsonNode add(final IJsonNode array, final IJsonNode node, final int index) {
		((IArrayNode) array).add(index, node);

		return array;
	}

	public static IJsonNode average(final INumericNode... inputs) {
		double sum = 0;

		for (final INumericNode numericNode : inputs)
			sum += ((DoubleNode) numericNode).getDoubleValue();

		return DoubleNode.valueOf(sum / inputs.length);
	}

	public static IJsonNode trim(final TextNode input) {
		return TextNode.valueOf(input.getTextValue().trim());
	}

	public static IJsonNode split(final TextNode input, final TextNode splitString) {
		final String[] split = input.getTextValue().split(splitString.getTextValue());
		final ArrayNode splitNode = new ArrayNode();
		for (final String string : split)
			splitNode.add(TextNode.valueOf(string));
		return splitNode;
	}

	public static IJsonNode extract(final TextNode input, final TextNode pattern, final IJsonNode defaultValue) {
		final Pattern compiledPattern = Pattern.compile(pattern.getTextValue());
		final Matcher matcher = compiledPattern.matcher(input.getTextValue());

		if (!matcher.find())
			return defaultValue;

		if (matcher.groupCount() == 0)
			return TextNode.valueOf(matcher.group(0));

		if (matcher.groupCount() == 1)
			return TextNode.valueOf(matcher.group(1));

		final ArrayNode result = new ArrayNode();
		for (int index = 1; index <= matcher.groupCount(); index++)
			result.add(TextNode.valueOf(matcher.group(index)));
		return result;
	}

	public static IJsonNode extract(final TextNode input, final TextNode pattern) {
		return extract(input, pattern, NullNode.getInstance());
	}

	public static IJsonNode replace(final TextNode input, final TextNode search, final TextNode replace) {
		return TextNode.valueOf(input.getTextValue().replaceAll(search.getTextValue(), replace.getTextValue()));
	}

	public static IJsonNode filter(final IArrayNode input, final IJsonNode... elementsToFilter) {
		final ArrayNode output = new ArrayNode();
		final HashSet<IJsonNode> filterSet = new HashSet<IJsonNode>(Arrays.asList(elementsToFilter));
		for (int index = 0; index < input.size(); index++)
			if (!filterSet.contains(input.get(index)))
				output.add(input.get(index));
		return output;
	}

	@Override
	public void registerFunctions(final MethodRegistry registry) {
		final List<Method> methods = ReflectUtil.getMethods(String.class, null, Modifier.PUBLIC, ~Modifier.STATIC);
		for (final Method method : methods)
			try {
				if (method.getDeclaringClass() == String.class)
					registry.register(method);
			} catch (final Exception e) {
				// System.out.println("Could not register " + method);
			}
	}

	@Override
	public void registerConstants(final EvaluationContext context) {
		context.getBindings().set("pi", new ConstantExpression(Math.PI));
		context.getBindings().set("e", new ConstantExpression(Math.E));
	}

	//
	// public static IJsonNode group(ArrayNode array, TextNode elementName) {
	// return extract(input, pattern, NullNode.getInstance());
	// }
	//
	// public static IJsonNode aggregate(ArrayNode array, EvaluationExpression groupingExpression, EvaluationExpression
	// aggregation) {
	// final List<CompactArrayNode> nodes = new ArrayList<CompactArrayNode>();
	// for (final IJsonNode jsonNode : array)
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
		protected IJsonNode aggregate(final IJsonNode aggregate, final IJsonNode node, final EvaluationContext context) {
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
		protected IJsonNode aggregate(final IJsonNode aggregate, final IJsonNode node, final EvaluationContext context) {
			if (aggregate.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(aggregate, node))
				return node;
			return aggregate;
		}
	};
}
