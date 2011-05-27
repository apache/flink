package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.Evaluable;

public abstract class ContainerExpression<E extends Evaluable> extends EvaluableExpression implements Iterable<E> {
	public void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment) {
		for (E element : this)
			if (element instanceof ContainerExpression)
				((ContainerExpression<?>) element).replace(toReplace, replaceFragment);
	}

	public <T extends Evaluable> T find(Class<T> evaluableClass) {
		for (E element : this) {
			if (evaluableClass.isInstance(element))
				return (T) element;

			if (element instanceof ContainerExpression) {
				T subSearch = ((ContainerExpression<?>) element).find(evaluableClass);
				if(subSearch != null) return subSearch;
			}
		}
		return null;
	}
}
