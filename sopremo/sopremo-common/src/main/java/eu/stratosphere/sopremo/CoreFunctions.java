package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.Aggregation;
import eu.stratosphere.sopremo.function.MaterializingAggregationFunction;
import eu.stratosphere.sopremo.function.TransitiveAggregationFunction;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.tokenizer.RegexTokenizer;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Core functions.
 * 
 * @author Arvid Heise
 */
public class CoreFunctions implements BuiltinProvider {
	@Name(verb = "concat", noun = "concatenation")
	public static final Aggregation<IJsonNode, TextNode> CONCAT =
		new TransitiveAggregationFunction<IJsonNode, TextNode>("concat", new TextNode()) {

			/**
		 * 
		 */
			private static final long serialVersionUID = -672755307899894156L;

			@Override
			public TextNode aggregate(IJsonNode node, TextNode aggregationTarget,
					EvaluationContext context) {
				aggregationTarget.append((TextNode) node);
				return aggregationTarget;
			}

			@Override
			public IJsonNode getFinalAggregate(TextNode aggregator, IJsonNode target) {
				TextNode textTarget = SopremoUtil.ensureType(target, TextNode.class);
				textTarget.copyValueFrom(aggregator);
				return textTarget;
			}
		};

	/**
	 * Repeatedly applies the {@link ArithmeticOperator#ADDITION} to the children of the given node.
	 */
	@Name(verb = "sum", noun = "sum")
	public static final Aggregation<INumericNode, INumericNode> SUM =
		new TransitiveAggregationFunction<INumericNode, INumericNode>(
			"sum", IntNode.ZERO) {
			/**
		 * 
		 */
			private static final long serialVersionUID = -8021932798231751696L;

			@Override
			public INumericNode aggregate(INumericNode node, INumericNode aggregationTarget, EvaluationContext context) {
				return ArithmeticOperator.ADDITION.evaluate(node, aggregationTarget, aggregationTarget);
			}
		};

	@Name(verb = "count", noun = "count")
	public static final Aggregation<IJsonNode, IntNode> COUNT = new TransitiveAggregationFunction<IJsonNode, IntNode>(
		"count", IntNode.ZERO) {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4700372075569392783L;

		@Override
		public IntNode aggregate(IJsonNode node, IntNode aggregationTarget, EvaluationContext context) {
			return (IntNode) ArithmeticOperator.ADDITION.evaluate(IntNode.ONE, aggregationTarget, aggregationTarget);
		}
	};

	@Name(noun = "first")
	public static final Aggregation<IJsonNode, IJsonNode> FIRST =
		new TransitiveAggregationFunction<IJsonNode, IJsonNode>(
			"first", NullNode.getInstance()) {
			/**
		 * 
		 */
			private static final long serialVersionUID = 273172975676646935L;

			@Override
			public IJsonNode aggregate(IJsonNode node, IJsonNode aggregationTarget, EvaluationContext context) {
				return aggregationTarget.isNull() ? node : aggregationTarget;
			}
		};

	@Name(verb = "sort")
	public static final Aggregation<IJsonNode, ArrayNode> SORT = new MaterializingAggregationFunction("sort") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3035270432104235038L;

		@Override
		protected IJsonNode processNodes(final IArrayNode nodeArray, final IJsonNode target) {
			final IJsonNode[] nodes = nodeArray.toArray();
			Arrays.sort(nodes);
			nodeArray.setAll(nodes);
			return nodeArray;
		}
	};

	@Name(adjective = "all")
	public static final Aggregation<IJsonNode, ArrayNode> ALL = new MaterializingAggregationFunction("all") {

		/**
		 * 
		 */
		private static final long serialVersionUID = 9079394721632933377L;
	};

	static final class AverageState extends ArrayNode {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6320534939283525418L;

		/**
		 * Initializes CoreFunctions.AverageState.
		 *
		 */
		public AverageState() {
			add(new IntNode(0));
			add(new IntNode(0));
		}
		
		public INumericNode getSum() {
			return (INumericNode) get(0);
		}
		
		public void setSum(INumericNode sum) {
			set(0, sum);
		}		
		
		public IntNode getCount() {
			return (IntNode) get(1);
		}
		
		public void increaseCount() {
			getCount().increment();
		}
	}
	
	@Name(noun = "average")
	public static final Aggregation<INumericNode, AverageState> AVERAGE = new Aggregation<INumericNode, AverageState>("avg") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 483420587993286076L;

		@Override
		public AverageState aggregate(INumericNode node, AverageState avgState, EvaluationContext context) {
			INumericNode sum = (INumericNode) avgState.get(0);
			avgState.setSum(ArithmeticOperator.ADDITION.evaluate(node, sum, sum));
			avgState.increaseCount();
			return avgState;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.aggregation.AggregationFunction#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode
		 * , eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		public INumericNode getFinalAggregate(AverageState avgState, IJsonNode target) {
			if (avgState.get(1).equals(IntNode.ZERO))
				return DoubleNode.NaN;
			return ArithmeticOperator.DIVISION.evaluate(avgState.getSum(), avgState.getCount(),				target);
		}

		@Override
		public AverageState initialize(AverageState aggregationValue) {
			return aggregationValue == null ? new AverageState() : aggregationValue;
		}
	};

	public static final EvaluationExpression PI = new ConstantExpression(Math.PI), E = new ConstantExpression(Math.E);

	@Name(noun = "min")
	public static final Aggregation<IJsonNode, IJsonNode> MIN =
		new TransitiveAggregationFunction<IJsonNode, IJsonNode>("min", NullNode.getInstance()) {
			/**
		 * 
		 */
			private static final long serialVersionUID = -8124401653435722884L;

			@Override
			public IJsonNode aggregate(final IJsonNode node, final IJsonNode aggregator, final EvaluationContext context) {
				if (aggregator.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(node, aggregator))
					return node;
				return aggregator;
			}
		};

	@Name(noun = "max")
	public static final Aggregation<IJsonNode, IJsonNode> MAX =
		new TransitiveAggregationFunction<IJsonNode, IJsonNode>("max", NullNode.getInstance()) {
			/**
		 * 
		 */
			private static final long serialVersionUID = -1735264603829085865L;

			@Override
			public IJsonNode aggregate(final IJsonNode node, final IJsonNode aggregator, final EvaluationContext context) {
				if (aggregator.isNull() || ComparativeExpression.BinaryOperator.LESS.evaluate(aggregator, node))
					return node;
				return aggregator;
			}
		};
		

		/**
		 * Creates a new array by combining sparse array information.<br />
		 * For example: [[0, "a"], [3, "d"], [2, "c"]] -&lt; ["a", missing, "c", "d"]
		 */
		@Name(verb = "assemble")
		public static final Aggregation<IArrayNode, ArrayNode> ASSEMBLE_ARRAY =
			new TransitiveAggregationFunction<IArrayNode, ArrayNode>(
				"assemble", new ArrayNode()) {
				/**
			 * 
			 */
				private static final long serialVersionUID = -8021932798231751696L;

				@Override
				public ArrayNode aggregate(IArrayNode node, ArrayNode aggregationTarget, EvaluationContext context) {
					aggregationTarget.add(((INumericNode) node.get(0)).getIntValue(), node.get(1));
					return aggregationTarget;
				}
			};

	/**
	 * Adds the specified node to the array at the given index
	 * 
	 * @param array
	 *        the array that should be extended
	 * @param index
	 *        the position of the insert
	 * @param node
	 *        the node to add
	 * @return array with the added node
	 */
	public static IArrayNode add(@SuppressWarnings("unused") final IArrayNode result, final IArrayNode array,
			final IntNode index, final IJsonNode node) {
		array.add(resolveIndex(index.getIntValue(), array.size()), node);
		return array;
	}

	public static void camelCase(final TextNode result, final TextNode input) {
		result.copyValueFrom(input);
		final char[] chars = result.asCharArray();

		boolean capitalize = true;
		for (int index = 0; index < chars.length; index++)
			if (Character.isWhitespace(chars[index]))
				capitalize = true;
			else if (capitalize) {
				chars[index] = Character.toUpperCase(chars[index]);
				capitalize = false;
			} else
				chars[index] = Character.toLowerCase(chars[index]);
	}

	/**
	 * Concatenates the textual representation of the nodes.
	 * 
	 * @param params
	 *        the nodes to concatenate
	 * @return a string node of the concatenated textual representations
	 */
	public static void concat(final TextNode result, final IJsonNode... params) {
		final StringBuilder builder = new StringBuilder();
		for (final IJsonNode jsonNode : params)
			builder.append((TextNode) jsonNode);
		result.setValue(builder.toString());
	}

	public static IJsonNode extract(final IJsonNode result, final TextNode input, final TextNode pattern) {
		return extract(result, input, pattern, NullNode.getInstance());
	}

	public static IJsonNode extract(final IJsonNode result, final TextNode input, final TextNode pattern,
			final IJsonNode defaultValue) {
		final Pattern compiledPattern = SopremoUtil.getPatternOf(pattern);
		final Matcher matcher = compiledPattern.matcher(input.getTextValue());

		if (!matcher.find())
			return defaultValue;

		if (matcher.groupCount() == 0) {
			TextNode stringResult = SopremoUtil.ensureType(result, TextNode.class);
			stringResult.setValue(matcher.group(0));
			return stringResult;
		}

		if (matcher.groupCount() == 1) {
			TextNode stringResult = SopremoUtil.ensureType(result, TextNode.class);
			stringResult.setValue(matcher.group(1));
			return stringResult;
		}

		final ArrayNode arrayResult = SopremoUtil.ensureType(result, ArrayNode.class);
		for (int index = 1; index <= matcher.groupCount(); index++)
			arrayResult.add(TextNode.valueOf(matcher.group(index)));
		return arrayResult;
	}

	public static void filter(final ArrayNode result, final IArrayNode input, final IJsonNode... elementsToFilter) {
		final HashSet<IJsonNode> filterSet = new HashSet<IJsonNode>(Arrays.asList(elementsToFilter));
		result.clear();
		for (int index = 0; index < input.size(); index++)
			if (!filterSet.contains(input.get(index)))
				result.add(input.get(index));
	}

	public static void format(final TextNode result, final TextNode format, final IJsonNode... params) {
		final Object[] paramsAsObjects = new Object[params.length];
		for (int index = 0; index < paramsAsObjects.length; index++)
			paramsAsObjects[index] =
				params[index].isTextual() ? ((TextNode) params[index]).getTextValue() : params[index].toString();

		result.clear();
		result.asFormatter().format(format.getTextValue().toString(), paramsAsObjects);
	}

	public static void length(final IntNode result, final TextNode node) {
		result.setValue(node.getTextValue().length());
	}

	public static void replace(final TextNode result, final TextNode input, final TextNode search,
			final TextNode replace) {
		final Pattern searchPattern = SopremoUtil.getPatternOf(search);
		final Matcher matcher = searchPattern.matcher(input);
		result.setValue(matcher.replaceAll(replace.toString()));
	}

	private static final Map<Pattern, RegexTokenizer> REGEX_TOKENIZERS = new IdentityHashMap<Pattern, RegexTokenizer>();

	public static void split(final CachingArrayNode result, final TextNode input, final TextNode splitString) {
		final Pattern searchPattern = SopremoUtil.getPatternOf(splitString);
		RegexTokenizer regexTokenizer = REGEX_TOKENIZERS.get(searchPattern);
		if (regexTokenizer == null)
			REGEX_TOKENIZERS.put(searchPattern, regexTokenizer = new RegexTokenizer(searchPattern));
		regexTokenizer.tokenizeInto(input, result);
	}
	
	private static final TextNode WHITESPACES = TextNode.valueOf("\\p{javaWhitespace}+");
	public static void split(final CachingArrayNode result, final TextNode input) {
		split(result, input, WHITESPACES);
	}

	public static void substring(final TextNode result, final TextNode input, final IntNode from, final IntNode to) {
		final int length = input.length();
		final int fromPos = resolveIndex(from.getIntValue(), length);
		final int toPos = resolveIndex(to.getIntValue(), length);

		result.setValue(input, fromPos, toPos);
	}

	public static void trim(final TextNode result, final TextNode input) {
		int start = 0, end = input.length() - 1;
		while(start < end && input.charAt(start) == ' ')
			start++;
		while(end > start && input.charAt(end) == ' ')
			end--;
			
		result.setValue(input, start, end + 1);
	}

	/**
	 * Concatenates the children of the given arrays.
	 * 
	 * @param arrays
	 *        the arrays to concatenate
	 * @return the concatenated array
	 */
	public static void unionAll(final ArrayNode union, final IJsonNode... arrays) {
		// boolean hasStream = false; // , resettable = false;
		// for (final IJsonNode param : arrays) {
		// final boolean stream = param instanceof ArrayNode;
		// hasStream |= stream;
		// // if (stream && ((ArrayNode) param).isResettable()) {
		// // resettable = true;
		// // break;
		// // }
		// }
		//
		// if (hasStream) {
		// final List<Iterator<IJsonNode>> iterators = new ArrayList<Iterator<IJsonNode>>(arrays.length);
		// for (int index = 0; index < arrays.length; index++)
		// iterators.add(((ArrayNode) arrays[index]).iterator());
		// return ArrayNode.valueOf(new ConcatenatingIterator<IJsonNode>(iterators)/* , resettable */);
		// }

		union.clear();
		for (final IJsonNode param : arrays)
			for (final IJsonNode child : (IArrayNode) param)
				union.add(child);
	}

	private static int resolveIndex(final int index, final int size) {
		if (index < 0)
			return size + index;
		return index;
	}
}
