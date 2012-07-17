package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This AggregationFunction aggregates the given nodes by simply creating an {@link IArrayNode} that contains all of
 * them.
 */
public class MaterializingAggregationFunction extends AggregationFunction {

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
	 * @see eu.stratosphere.sopremo.aggregation.AggregationFunction#aggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode aggregate(IJsonNode node, IJsonNode aggregationValue, EvaluationContext context) {
		((IArrayNode) aggregationValue).add(node);
		return aggregationValue;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.AggregationFunction#initialize(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode initialize(IJsonNode aggregationValue) {
		return SopremoUtil.reinitializeTarget(aggregationValue, ArrayNode.class);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.aggregation.AggregationFunction#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode getFinalAggregate(IJsonNode aggregator, IJsonNode target) {
		return this.processNodes((IArrayNode) aggregator, target);
	}

	protected IJsonNode processNodes(final IArrayNode nodeArray, @SuppressWarnings("unused") IJsonNode target) {
		return nodeArray;
	}
}
