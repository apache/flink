package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;

public abstract class ContainerExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	@SuppressWarnings("unchecked")
	public <T extends EvaluationExpression> T find(final Class<T> evaluableClass) {
		for (final SopremoExpression<EvaluationContext> element : this) {
			if (evaluableClass.isInstance(element))
				return (T) element;

			if (element instanceof ContainerExpression) {
				final T subSearch = ((ContainerExpression) element).find(evaluableClass);
				if (subSearch != null)
					return subSearch;
			}
		}
		return null;
	}

	@Override
	public abstract Iterator<SopremoExpression<EvaluationContext>> iterator();

	public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		for (final SopremoExpression<EvaluationContext> element : this)
			if (element instanceof ContainerExpression)
				((ContainerExpression) element).replace(toReplace, replaceFragment);
	}
}
