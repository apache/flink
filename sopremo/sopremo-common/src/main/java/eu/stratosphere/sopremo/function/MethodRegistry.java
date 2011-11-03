package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.Bindings;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.FunctionRegistryCallback;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

public class MethodRegistry extends AbstractSopremoType implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8399369017331739066L;

	private Bindings bindings;

	public MethodRegistry(Bindings bindings) {
		this.bindings = bindings;
	}

	public JsonNode evaluate(final String functionName, final ArrayNode params, final EvaluationContext context) {
		final JsonMethod function = this.getFunction(functionName);
		if (function == null)
			throw new EvaluationException(String.format("Unknown function %s", functionName));
		return function.call(params, context);
	}

	public JsonMethod getFunction(final String functionName) {
		return this.bindings.get(functionName, JsonMethod.class);
	}

	Map<String, JsonMethod> getRegisteredFunctions() {
		return this.bindings.getAll(JsonMethod.class);
	}

	private static boolean isCompatibleSignature(final Method method) {
		if (!JsonNode.class.isAssignableFrom(method.getReturnType()))
			return false;

		boolean compatibleSignature;
		Class<?>[] parameterTypes = method.getParameterTypes();
		if (parameterTypes.length == 1 && parameterTypes[0].isArray()
			&& JsonNode.class.isAssignableFrom(parameterTypes[0].getComponentType()))
			compatibleSignature = true;
		else {
			compatibleSignature = true;
			for (int index = 0; index < parameterTypes.length; index++)
				if (!JsonNode.class.isAssignableFrom(parameterTypes[index])
					&& !(index == parameterTypes.length - 1 && method.isVarArgs() &&
					JsonNode.class.isAssignableFrom(parameterTypes[index].getComponentType()))) {
					compatibleSignature = false;
					break;
				}
		}
		return compatibleSignature;
	}

	public void register(final Class<?> javaFunctions) {
		List<Method> functions = getCompatibleMethods(
			ReflectUtil.getMethods(javaFunctions, null, Modifier.STATIC | Modifier.PUBLIC));

		for (Method method : functions)
			registerInternal(method);

		if (FunctionRegistryCallback.class.isAssignableFrom(javaFunctions))
			((FunctionRegistryCallback) ReflectUtil.newInstance(javaFunctions)).registerFunctions(this);
	}

	public void register(Method method) {
		registerInternal(method);
	}

	private void registerInternal(Method method) {
		JavaMethod javaFunction = this.bindings.get(method.getName(), JavaMethod.class);
		if (javaFunction == null)
			this.bindings.set(method.getName(), javaFunction = new JavaMethod(method.getName()));
		javaFunction.addSignature(method);
	}

	public static List<Method> getCompatibleMethods(List<Method> methods) {
		List<Method> functions = new ArrayList<Method>();
		for (Method method : methods)
			if (isCompatibleSignature(method))
				functions.add(method);
		return functions;
	}

	public void register(final Callable<?, ?> function) {
		this.bindings.set(function.getName(), function);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		this.bindings.toString(builder);
	}
}
