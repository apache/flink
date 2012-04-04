package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

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
		this.expectedTarget = ArrayNode.class;
	}

	/**
	 * Initializes ArrayCreation to create an array of the given expressions.
	 * 
	 * @param elements
	 *        the expressions that evaluate to the elements in the array
	 */
	public ArrayCreation(final List<EvaluationExpression> elements) {
		this.elements = elements.toArray(new EvaluationExpression[elements.size()]);
		this.expectedTarget = ArrayNode.class;
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
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		target = SopremoUtil.reuseTarget(target, this.expectedTarget);

		int index = 0;

		for (final EvaluationExpression expression : this.elements)
			((IArrayNode) target).add(expression.evaluate(node, ((IArrayNode) target).get(index++), context));

		return target;

	}

	@Override
	public int hashCode() {
		return 53 * super.hashCode() + Arrays.hashCode(this.elements);
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
	public List<? extends EvaluationExpression> getChildren() {
		return Arrays.asList(this.elements);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(final List<? extends EvaluationExpression> children) {
		this.elements = children.toArray(this.elements);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(Arrays.toString(this.elements));
	}
}