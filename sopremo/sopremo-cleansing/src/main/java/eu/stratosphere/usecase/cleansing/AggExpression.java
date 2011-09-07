package eu.stratosphere.usecase.cleansing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;

public class AggExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7602198150833087978L;

	private EvaluationExpression groupingExpression, resultExpression;

	public AggExpression(EvaluationExpression groupingExpression, EvaluationExpression resultExpression) {
		this.groupingExpression = groupingExpression;
		this.resultExpression = resultExpression;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (node.size() == 0)
			return new ArrayNode(null);

		final List<CompactArrayNode> nodes = this.sortNodesWithKey(node, context);

		final ArrayNode resultNode = new ArrayNode(null);

		int groupStart = 0;
		JsonNode groupKey = nodes.get(0).get(0);
		for (int index = 1; index < nodes.size(); index++)
			if (!nodes.get(index).get(0).equals(groupKey)) {
				resultNode.add(this.evaluateGroup(nodes.subList(groupStart, index), context));
				groupKey = nodes.get(index).get(0);
				groupStart = index;
			}

		resultNode.add(this.evaluateGroup(nodes.subList(groupStart, nodes.size()), context));

		return resultNode;
	}

	protected List<CompactArrayNode> sortNodesWithKey(JsonNode node, EvaluationContext context) {
		final List<CompactArrayNode> nodes = new ArrayList<CompactArrayNode>();
		for (final JsonNode jsonNode : node)
			nodes.add(JsonUtil.asArray(this.groupingExpression.evaluate(jsonNode, context), jsonNode));
		Collections.sort(nodes, new Comparator<CompactArrayNode>() {
			@Override
			public int compare(CompactArrayNode o1, CompactArrayNode o2) {
				return JsonNodeComparator.INSTANCE.compare(o1.get(0), o2.get(0));
			}
		});
		return nodes;
	}

	protected JsonNode evaluateGroup(List<CompactArrayNode> group, EvaluationContext context) {
		ArrayNode values = new ArrayNode(null);
		for (CompactArrayNode compactArrayNode : group)
			values.add(compactArrayNode.get(1));
		return this.resultExpression.evaluate(values, context);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.groupingExpression.hashCode();
		result = prime * result + this.resultExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		AggExpression other = (AggExpression) obj;
		return this.groupingExpression.equals(other.groupingExpression)
			&& this.resultExpression.equals(other.resultExpression);
	}
	
	@Override
	protected void toString(StringBuilder builder) {
		builder.append("g(").append(groupingExpression).append(") -> ").append(resultExpression);
	}

}
