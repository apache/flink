package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class VarReturnJavaMethod extends JavaMethod {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	/**
	 * Initializes VarReturnJavaMethod.
	 * 
	 * @param name
	 */
	public VarReturnJavaMethod(String name) {
		super(name);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(InputType[], eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode call(final IArrayNode params, final IJsonNode target, final EvaluationContext context) {
		try {
			return this.method.invoke(null, addTargetToParameters(params, target));
		} catch (Exception e) {
			throw new EvaluationException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Java method ").append(this.method);
	}
}
