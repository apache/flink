package eu.stratosphere.sopremo.aggregation;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class MaterializingAggregationFunction extends AggregationFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3685213903416162250L;

	private transient List<JsonNode> nodes;

	public MaterializingAggregationFunction() {
		super("<values>");
	}

	protected MaterializingAggregationFunction(final String name) {
		super(name);
	}

	@Override
	public void aggregate(final JsonNode node, final EvaluationContext context) {
		this.nodes.add(node);
	}

	@Override
	public JsonNode getFinalAggregate() {
		final ArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(this.processNodes(this.nodes));
		this.nodes = null;
		return arrayNode;
	}

	@Override
	public void initialize() {
		this.nodes = new ArrayList<JsonNode>();
	}

	protected List<JsonNode> processNodes(final List<JsonNode> nodes) {
		return nodes;
	}
}
