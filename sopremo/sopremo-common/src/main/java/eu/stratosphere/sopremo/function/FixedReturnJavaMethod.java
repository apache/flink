package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class FixedReturnJavaMethod<ReturnType extends IJsonNode> extends JavaMethod {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	private final ReturnType returnValue;

	private ObjectArrayList<Object> parameters = new ObjectArrayList<Object>();

	public FixedReturnJavaMethod(final String name, final ReturnType returnValue) {
		super(name);
		this.returnValue = returnValue;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.JavaMethod#addSignature(java.lang.reflect.Method)
	 */
	@Override
	public void addSignature(Method method) {
		if (method.getReturnType() != Void.TYPE || method.getParameterTypes()[0] != this.returnValue.getClass())
			throw new IllegalArgumentException("Can't overload functions with different return types");
		super.addSignature(method);
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
		elements[0] = this.returnValue;
		for (int index = 0; index < numParams; index++) 
			elements[index + 1] = params.get(index); 
		this.method.invoke(null, elements);
		return this.returnValue;
	}
}
