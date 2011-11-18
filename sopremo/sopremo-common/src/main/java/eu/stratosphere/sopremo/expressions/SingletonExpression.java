package eu.stratosphere.sopremo.expressions;

public abstract class SingletonExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4108217673663116837L;

	@Override
	public boolean equals(Object obj) {
		return obj == this;
	}
	
	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}
	
	protected abstract Object readResolve();
}
