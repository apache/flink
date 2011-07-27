package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.aggregation.MaterializingAggregationFunction;
import eu.stratosphere.sopremo.aggregation.TransitiveAggregationFunction;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.OptimizerHints;
import eu.stratosphere.sopremo.expressions.Scope;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.util.ConcatenatingIterator;

/**
 * Base built-in functions.
 * 
 * @author Arvid Heise
 */
public class BuiltinFunctions {
	private static final NumericNode ZERO = new IntNode(0), ONE = new IntNode(1);

	/**
	 * Concatenates the textual representation of the nodes.
	 * 
	 * @param params
	 *        the nodes to concatenate
	 * @return a string node of the concatenated textual representations
	 */
	@OptimizerHints(minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode concat(JsonNode[] params) {
		StringBuilder builder = new StringBuilder();
		for (JsonNode jsonNode : params)
			builder.append(jsonNode.isTextual() ? jsonNode.getTextValue() : jsonNode);
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
	public static JsonNode count(JsonNode node) {
		return new IntNode(node.size());
	}

	@OptimizerHints(scope = Scope.ARRAY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode sort(JsonNode node) {
		List<JsonNode> nodes = new ArrayList<JsonNode>();
		for (JsonNode jsonNode : node)
			nodes.add(jsonNode);
		Collections.sort(nodes, JsonNodeComparator.INSTANCE);
		ArrayNode arrayNode = new ArrayNode(null);
		arrayNode.addAll(nodes);
		return arrayNode;
	}

	/**
	 * Repeatedly applies the {@link ArithmeticOperator#ADDITION} to the children of the given node.
	 * 
	 * @param node
	 *        the children of this node are summed up
	 * @return the sum of child elements
	 */
	@OptimizerHints(minNodes = 0, maxNodes = OptimizerHints.UNBOUND, transitive = true, iterating = true)
	public static JsonNode sum(JsonNode node) {
		Iterator<JsonNode> iterator = node.iterator();
		if (!iterator.hasNext())
			return ZERO;
		NumericNode sum = (NumericNode) iterator.next();
		for (; iterator.hasNext();)
			sum = ArithmeticExpression.ArithmeticOperator.ADDITION.evaluate((NumericNode) sum,
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
	public static JsonNode unionAll(JsonNode... arrays) {
		boolean hasStream = false, resettable = false;
		for (JsonNode param : arrays) {
			boolean stream = param instanceof StreamArrayNode;
			hasStream |= stream;
			if (stream && ((StreamArrayNode) param).isResettable()) {
				resettable = true;
				break;
			}
		}

		if (hasStream) {
			Iterator<?>[] iterators = new Iterator[arrays.length];
			for (int index = 0; index < iterators.length; index++)
				iterators[index] = arrays[index].iterator();
			return StreamArrayNode.valueOf(new ConcatenatingIterator<JsonNode>(iterators), resettable);
		}

		ArrayNode union = JsonUtil.NODE_FACTORY.arrayNode();
		for (JsonNode param : arrays)
			for (JsonNode child : param)
				union.add(child);
		return union;
	}

	public static final AggregationFunction SUM = new TransitiveAggregationFunction("sum", ZERO) {
		@Override
		protected JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, (NumericNode) node);
		}
	};

	public static final AggregationFunction COUNT = new TransitiveAggregationFunction("count", ZERO) {
		@Override
		protected JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context) {
			return ArithmeticOperator.ADDITION.evaluate((NumericNode) aggregate, ONE);
		}
	};

	public static final AggregationFunction FIRST = new TransitiveAggregationFunction("first", NullNode.getInstance()) {
		@Override
		protected JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context) {
			return aggregate.isNull() ? node : aggregate;
		}
	};

	public static final AggregationFunction SORT = new MaterializingAggregationFunction("sort") {
		protected List<JsonNode> processNodes(List<JsonNode> nodes) {
			Collections.sort(nodes, JsonNodeComparator.INSTANCE);
			return nodes;
		}
	};

	public static final AggregationFunction AVERAGE = new AggregationFunction("avg") {
		private transient int count;

		private transient double value;

		@Override
		public JsonNode getFinalAggregate() {
			if (count == 0)
				return DoubleNode.valueOf(Double.NaN);
			return DoubleNode.valueOf(value / count);
		}

		@Override
		public void initialize() {
			count = 0;
			value = 0;
		}

		@Override
		public void aggregate(JsonNode node, EvaluationContext context) {
			value += node.getDoubleValue();
			count++;
		}
	};
}
