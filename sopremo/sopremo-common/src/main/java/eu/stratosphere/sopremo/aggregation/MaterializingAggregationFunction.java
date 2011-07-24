package eu.stratosphere.sopremo.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;

public class MaterializingAggregationFunction extends AggregationFunction {

	protected MaterializingAggregationFunction(String name) {
		super(name);
	}

	public MaterializingAggregationFunction() {
		super("<values>");
	}

	private transient List<JsonNode> nodes;

	@Override
	public void initialize() {
		nodes = new ArrayList<JsonNode>();
	}

	@Override
	public void aggregate(JsonNode node, EvaluationContext context) {
		nodes.add(node);
	}

	@Override
	public JsonNode getFinalAggregate() {
		ArrayNode arrayNode = new ArrayNode(null);
		arrayNode.addAll(processNodes(nodes));
		return arrayNode;
	}

	protected List<JsonNode> processNodes(List<JsonNode> nodes) {
		return nodes;
	}
}
