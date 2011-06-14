package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.Evaluable;

public abstract class ContainerExpression extends EvaluableExpression implements Iterable<EvaluableExpression> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	public <T extends Evaluable> T find(Class<T> evaluableClass) {
		for (EvaluableExpression element : this) {
			if (evaluableClass.isInstance(element))
				return (T) element;

			if (element instanceof ContainerExpression) {
				T subSearch = ((ContainerExpression) element).find(evaluableClass);
				if (subSearch != null)
					return subSearch;
			}
		}
		return null;
	}

	public void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment) {
		for (EvaluableExpression element : this)
			if (element instanceof ContainerExpression)
				((ContainerExpression) element).replace(toReplace, replaceFragment);
	}
}
