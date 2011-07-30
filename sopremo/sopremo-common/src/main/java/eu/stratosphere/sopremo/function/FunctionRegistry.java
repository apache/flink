package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;

public class FunctionRegistry implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8399369017331739066L;

	private Map<String, Function> registeredFunctions = new HashMap<String, Function>();

	public FunctionRegistry() {
	}

	public JsonNode evaluate(String functionName, JsonNode node, EvaluationContext context) {
		return this.getFunction(functionName).evaluate(node, context);
	}

	public Function getFunction(String functionName) {
		return this.registeredFunctions.get(functionName);
	}

	Map<String, Function> getRegisteredFunctions() {
		return this.registeredFunctions;
	}

	private boolean isCompatibleSignature(Method method, Class<?>[] parameterTypes) {
		boolean compatibleSignature;
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

	public void register(Class<?> javaFunctions) {
		for (Method method : javaFunctions.getDeclaredMethods())
			if ((method.getModifiers() & Modifier.STATIC) != 0
				&& JsonNode.class.isAssignableFrom(method.getReturnType())) {
				Class<?>[] parameterTypes = method.getParameterTypes();
				if (!this.isCompatibleSignature(method, parameterTypes))
					continue;
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

	public void register(Function function) {
		this.registeredFunctions.put(function.getName(), function);
	}
}
