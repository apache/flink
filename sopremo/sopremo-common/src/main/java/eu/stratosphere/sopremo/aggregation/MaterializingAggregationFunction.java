package eu.stratosphere.sopremo.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;

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
		final ArrayNode arrayNode = new ArrayNode(null);
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
