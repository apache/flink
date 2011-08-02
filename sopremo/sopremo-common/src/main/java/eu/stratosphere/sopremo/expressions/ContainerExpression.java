package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import eu.stratosphere.sopremo.Evaluable;

public abstract class ContainerExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	public <T extends Evaluable> T find(Class<T> evaluableClass) {
		for (EvaluationExpression element : this) {
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

	@Override
	public abstract Iterator<EvaluationExpression> iterator();

	public void replace(EvaluationExpression toReplace, EvaluationExpression replaceFragment) {
		for (EvaluationExpression element : this)
			if (element instanceof ContainerExpression)
				((ContainerExpression) element).replace(toReplace, replaceFragment);
	}
}
