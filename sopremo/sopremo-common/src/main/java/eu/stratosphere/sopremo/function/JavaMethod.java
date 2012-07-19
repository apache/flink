package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.util.Collection;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.DynamicMethod;
import eu.stratosphere.util.reflect.ExtensionMethod;
import eu.stratosphere.util.reflect.Signature;

public class JavaMethod extends SopremoMethod {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	private final DynamicMethod<IJsonNode> method;

	public JavaMethod(final String name) {
		this.method = new ExtensionMethod<IJsonNode>(name);
	}

	public void addSignature(final Method method) {
		this.method.addSignature(method);
	}

	public Collection<Signature> getSignatures() {
		return this.method.getSignatures();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(InputType[], eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode call(final IArrayNode params, final IJsonNode target, final EvaluationContext context) {
		return this.method.invoke(null, (Object[]) params.toArray());
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
