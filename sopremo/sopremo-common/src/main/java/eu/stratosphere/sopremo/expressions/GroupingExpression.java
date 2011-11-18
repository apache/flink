package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public class GroupingExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7602198150833087978L;

	private EvaluationExpression groupingExpression, resultExpression;

	public GroupingExpression(EvaluationExpression groupingExpression, EvaluationExpression resultExpression) {
		this.groupingExpression = groupingExpression;
		this.resultExpression = resultExpression;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (((ArrayNode) node).size() == 0)
			return new ArrayNode();

		final List<ArrayNode> nodes = this.sortNodesWithKey(node, context);

		final ArrayNode resultNode = new ArrayNode();

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

	protected List<ArrayNode> sortNodesWithKey(JsonNode node, EvaluationContext context) {
		final List<ArrayNode> nodes = new ArrayList<ArrayNode>();
		for (final JsonNode jsonNode : (ArrayNode) node)
			nodes.add(JsonUtil.asArray(this.groupingExpression.evaluate(jsonNode, context), jsonNode));
		Collections.sort(nodes, new Comparator<ArrayNode>() {
			@Override
			public int compare(ArrayNode o1, ArrayNode o2) {
				return o1.get(0).compareTo(o2.get(0));
			}
		});
		return nodes;
	}

	protected JsonNode evaluateGroup(List<ArrayNode> group, EvaluationContext context) {
		ArrayNode values = new ArrayNode();
		for (ArrayNode compactArrayNode : group)
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
		if (!super.equals(obj))
			return false;
		GroupingExpression other = (GroupingExpression) obj;
		return this.groupingExpression.equals(other.groupingExpression)
			&& this.resultExpression.equals(other.resultExpression);
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("g(").append(this.groupingExpression).append(") -> ").append(this.resultExpression);
	}

}
