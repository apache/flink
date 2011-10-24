package eu.stratosphere.sopremo.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JavaToJsonMapper;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.reflect.DynamicMethod;
import eu.stratosphere.util.reflect.Signature;

public class JavaMethod extends MethodBase {
	private final class AutoBoxingMethod extends DynamicMethod<JsonNode> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9145091116604733007L;

		private AutoBoxingMethod(String name) {
			super(name);
		}

		@Override
		protected Class<?>[] getSignatureTypes(Method member) {
			Class<?>[] parameterTypes = super.getParameterTypes(member);
			for (int index = 0; index < parameterTypes.length; index++)
				if (!parameterTypes[index].isArray())
					parameterTypes[index] = JavaToJsonMapper.INSTANCE.classToJsonType(parameterTypes[index]);
			return parameterTypes;
		}

		@Override
		protected JsonNode invokeDirectly(Method method, Object context, Object[] params)
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

	private DynamicMethod<JsonNode> method;

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

	@Override
	public JsonNode evaluate(final JsonNode targetNode, final ArrayNode paramNode, final EvaluationContext context) {
		return this.method.invoke(targetNode, (Object[]) paramNode.toArray());
	}
}
