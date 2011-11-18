package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * Creates an array of the given expressions.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.ANY)
public class ArrayCreation extends ContainerExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1681947333740209285L;

	private EvaluationExpression[] elements;

	/**
	 * Initializes ArrayCreation to create an array of the given expressions.
	 * 
	 * @param elements
	 *        the expressions that evaluate to the elements in the array
	 */
	public ArrayCreation(final EvaluationExpression... elements) {
		this.elements = elements;
	}

	/**
	 * Initializes ArrayCreation to create an array of the given expressions.
	 * 
	 * @param elements
	 *        the expressions that evaluate to the elements in the array
	 */
	public ArrayCreation(final List<EvaluationExpression> elements) {
		this.elements = elements.toArray(new EvaluationExpression[elements.size()]);
	}

	public int size() {
		return this.elements.length;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ArrayCreation other = (ArrayCreation) obj;
		return Arrays.equals(this.elements, other.elements);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final ArrayNode arrayNode = new ArrayNode();
		for (final EvaluationExpression expression : this.elements)
			arrayNode.add(expression.evaluate(node, context));
		return arrayNode;
	}

	@Override
	public int hashCode() {
		return 53 + Arrays.hashCode(this.elements);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this.elements).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<EvaluationExpression> getChildren() {
		return Arrays.asList(this.elements);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(List<EvaluationExpression> children) {
		this.elements = children.toArray(this.elements);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(Arrays.toString(this.elements));
	}
}