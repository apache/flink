package eu.stratosphere.sopremo.expressions;

import java.util.List;

/**
 * Represents an expression which holds a set of child-expressions.
 */
public abstract class ContainerExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	/**
	 * Returns this containers child-expressions.
	 * 
	 * @return this containers children
	 */
	public abstract List<? extends EvaluationExpression> getChildren();

	/**
	 * Sets this containers children.
	 * 
	 * @param children
	 *        all child-expressions that should be set
	 */
	public abstract void setChildren(List<? extends EvaluationExpression> children);

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(final TransformFunction function) {
		this.setChildren(this.transformChildExpressions(function, this.getChildren()));
		return function.call(this);
	}
}
