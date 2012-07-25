package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class MaterializingAggregationFunction extends Aggregation<IJsonNode, ArrayNode> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3685213903416162250L;

	/**
	 * Initializes a new MaterializingAggregationFunction with the name <code>"&#60values&#62"</code>.
	 */
	public MaterializingAggregationFunction() {
		super("<values>");
	}

	/**
	 * Initializes a new MaterializingAggregationFunction with the given name.
	 * 
	 * @param name
	 *        the name that should be used
	 */
	protected MaterializingAggregationFunction(final String name) {
		super(name);
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.aggregation.AggregationFunction#aggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 *      eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public ArrayNode aggregate(IJsonNode node, ArrayNode aggregationValue, EvaluationContext context) {
		aggregationValue.add(node);
		return aggregationValue;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.AggregationFunction#initialize(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ArrayNode initialize(ArrayNode aggregationValue) {
		return SopremoUtil.reinitializeTarget(aggregationValue, ArrayNode.class);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.aggregation.AggregationFunction#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode getFinalAggregate(ArrayNode aggregator, IJsonNode target) {
		return this.processNodes(aggregator, target);
	}

	protected IJsonNode processNodes(final IArrayNode nodeArray, @SuppressWarnings("unused") final IJsonNode target) {
		return nodeArray;
	}
}
