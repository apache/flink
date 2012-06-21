package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;
import java.util.List;

/**
 * Represents an expression which holds a set of child-expressions.
 */
public abstract class ContainerExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	@Override
	public abstract Iterator<EvaluationExpression> iterator();

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

	// /**
	// * Replaces the one expression in this container with another one.
	// *
	// * @param toReplace
	// * the expression that should be replaced
	// * @param replaceFragment
	// * the expression which should replace another one
	// */
	// public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
	// for (final EvaluationExpression element : this)
	// if (element instanceof ContainerExpression)
	// ((ContainerExpression) element).replace(toReplace, replaceFragment);
	// }

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public abstract EvaluationExpression transformRecursively(TransformFunction function);
}
