package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.util.reflect.ReflectUtil;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class FunctionRegistry implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8399369017331739066L;

	private final Map<String, Function> registeredFunctions = new HashMap<String, Function>();

	public FunctionRegistry() {
	}

	public JsonNode evaluate(final String functionName, final JsonNode node, final EvaluationContext context) {
		final Function function = this.getFunction(functionName);
		if (function == null)
			throw new EvaluationException(String.format("Unknown function %s", functionName));
		return function.evaluate(node, context);
	}

	public Function getFunction(final String functionName) {
		return this.registeredFunctions.get(functionName);
	}

	Map<String, Function> getRegisteredFunctions() {
		return this.registeredFunctions;
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

		for (Method method : functions) {
			Function javaFunction = this.registeredFunctions.get(method.getName());
			if (javaFunction == null)
				this.registeredFunctions.put(method.getName(), javaFunction = new JavaFunction(method.getName()));
			else if (!(javaFunction instanceof JavaFunction))
				throw new IllegalArgumentException(String.format(
					"a function with the name %s is already registered and not a java function: %s",
					method.getName(), javaFunction));
			((JavaFunction) javaFunction).addSignature(method);
		}
	}

	public static List<Method> getCompatibleMethods(List<Method> methods) {
		List<Method> functions = new ArrayList<Method>();
		for (Method method : methods)
			if (isCompatibleSignature(method))
				functions.add(method);
		return functions;
	}

	public void register(final Function function) {
		this.registeredFunctions.put(function.getName(), function);
	}
}
