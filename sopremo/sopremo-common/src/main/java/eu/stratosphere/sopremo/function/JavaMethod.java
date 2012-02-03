package eu.stratosphere.sopremo.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JavaToJsonMapper;
import eu.stratosphere.util.reflect.DynamicMethod;
import eu.stratosphere.util.reflect.ExtensionMethod;
import eu.stratosphere.util.reflect.Signature;

public class JavaMethod extends JsonMethod {
	private final class AutoBoxingMethod extends ExtensionMethod<IJsonNode> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9145091116604733007L;

		private AutoBoxingMethod(final String name) {
			super(name);
		}

		@Override
		protected Class<?>[] getSignatureTypes(final Method member) {
			final Class<?>[] parameterTypes = super.getParameterTypes(member);
			for (int index = 0; index < parameterTypes.length; index++)
				if (!parameterTypes[index].isArray())
					parameterTypes[index] = JavaToJsonMapper.INSTANCE.classToJsonType(parameterTypes[index]);
			return parameterTypes;
		}

		@Override
		protected IJsonNode invokeDirectly(final Method method, final Object context, final Object[] params)
				throws IllegalAccessException, InvocationTargetException {
			for (int index = 0; index < params.length; index++)
				if (!params[index].getClass().isArray())
					params[index] = JavaToJsonMapper.INSTANCE.valueToTree(params[index]);
			return JavaToJsonMapper.INSTANCE.valueToTree(super.invokeDirectly(method, context, params));
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	private final DynamicMethod<IJsonNode> method;

	public JavaMethod(final String name) {
		super(name);

		this.method = new AutoBoxingMethod(name);
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
	public IJsonNode call(final IArrayNode params, final EvaluationContext context) {
		return this.method.invoke(null, (Object[]) params.toArray());
	}
}
