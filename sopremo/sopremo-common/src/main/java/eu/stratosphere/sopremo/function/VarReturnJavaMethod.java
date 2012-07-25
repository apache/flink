package eu.stratosphere.sopremo.function;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class VarReturnJavaMethod extends JavaMethod {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	private ObjectArrayList<Object> parameters = new ObjectArrayList<Object>();

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
		final int numParams = params.size();
		this.parameters.size(numParams + 1);
		final Object[] elements = this.parameters.elements();
		elements[0] = target;
		for (int index = 0; index < numParams; index++) 
			elements[index + 1] = params.get(index); 
		return this.method.invoke(null, elements);
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
