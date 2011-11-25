package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;
import java.util.List;

public abstract class ContainerExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2109874880435636612L;

	@Override
	public abstract Iterator<EvaluationExpression> iterator();

	public abstract List<? extends EvaluationExpression> getChildren();
	
	public abstract void setChildren(List<? extends EvaluationExpression> children);

	public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		for (final EvaluationExpression element : this)
			if (element instanceof ContainerExpression)
				((ContainerExpression) element).replace(toReplace, replaceFragment);
	}
}
