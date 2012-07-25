package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Returns a grouped representation of the elements of the given {@link IArrayNode}.
 */
public class GroupingExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7602198150833087978L;

	private EvaluationExpression groupingExpression;

	private EvaluationExpression resultExpression;

	/**
	 * Initializes a GroupingExpression with the given expressions.
	 * 
	 * @param groupingExpression
	 *        the expression that should be used to determine the grouping keys
	 * @param resultExpression
	 *        the expression that should be used on the elements within a group
	 */
	public GroupingExpression(final EvaluationExpression groupingExpression, final EvaluationExpression resultExpression) {
		this.groupingExpression = groupingExpression;
		this.resultExpression = resultExpression;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		ArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);

		if (((IArrayNode) node).size() == 0) 
			return targetArray;

		final List<IArrayNode> nodes = this.sortNodesWithKey(node, context);

		// final ArrayNode resultNode = new ArrayNode();

		int groupStart = 0;
		IJsonNode groupKey = nodes.get(0).get(0);
		for (int index = 1; index < nodes.size(); index++)
			if (!nodes.get(index).get(0).equals(groupKey)) {
				targetArray.add(this.evaluateGroup(nodes.subList(groupStart, index), context));
				groupKey = nodes.get(index).get(0);
				groupStart = index;
			}

		targetArray.add(this.evaluateGroup(nodes.subList(groupStart, nodes.size()), context));

		return targetArray;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		this.groupingExpression = this.groupingExpression.transformRecursively(function);
		this.resultExpression = this.resultExpression.transformRecursively(function);
		return function.call(this);
	}

	protected List<IArrayNode> sortNodesWithKey(final IJsonNode node, final EvaluationContext context) {
		final List<IArrayNode> nodes = new ArrayList<IArrayNode>();
		for (final IJsonNode jsonNode : (IArrayNode) node)
			nodes.add(JsonUtil.asArray(this.groupingExpression.evaluate(jsonNode, null, context), jsonNode));
		Collections.sort(nodes, new Comparator<IArrayNode>() {
			@Override
			public int compare(final IArrayNode o1, final IArrayNode o2) {
				return o1.get(0).compareTo(o2.get(0));
			}
		});
		return nodes;
	}

	protected IJsonNode evaluateGroup(final List<IArrayNode> group, final EvaluationContext context) {
		final ArrayNode values = new ArrayNode();
		for (final IArrayNode compactArrayNode : group)
			values.add(compactArrayNode.get(1));
		return this.resultExpression.evaluate(values, null, context);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.groupingExpression.hashCode();
		result = prime * result + this.resultExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final GroupingExpression other = (GroupingExpression) obj;
		return this.groupingExpression.equals(other.groupingExpression)
			&& this.resultExpression.equals(other.resultExpression);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("g(").append(this.groupingExpression).append(") -> ").append(this.resultExpression);
	}

}
