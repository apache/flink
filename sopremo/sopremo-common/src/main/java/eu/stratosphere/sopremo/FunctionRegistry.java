package eu.stratosphere.sopremo;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;

public class FunctionRegistry implements SopremoType {
	private Map<String, Function> registeredFunctions = new HashMap<String, Function>();

	public void register(Function function) {
		registeredFunctions.put(function.getName(), function);
	}

	public JsonNode evaluate(String functionName, JsonNode node) {
		return getFunction(functionName).evaluate(node);
	}

	public Function getFunction(String functionName) {
		return registeredFunctions.get(functionName);
	}

	Map<String, Function> getRegisteredFunctions() {
		return registeredFunctions;
	}

	public void register(Class<?> javaFunctions) {
		methodFinder: for (Method method : javaFunctions.getDeclaredMethods()) {
			if ((method.getModifiers() & Modifier.STATIC) != 0
				&& JsonNode.class.isAssignableFrom(method.getReturnType())) {
				Class<?>[] parameterTypes = method.getParameterTypes();
				for (int index = 0; index < parameterTypes.length; index++) {
					if (!JsonNode.class.isAssignableFrom(parameterTypes[index])
						&& !(index == parameterTypes.length - 1 && method.isVarArgs() &&
								JsonNode.class.isAssignableFrom(parameterTypes[index].getComponentType())))
						continue methodFinder;
				}

				Function javaFunction = registeredFunctions.get(method.getName());
				if (javaFunction == null)
					registeredFunctions.put(method.getName(), javaFunction = new JavaFunction(method.getName()));
				else if (!(javaFunction instanceof JavaFunction))
					throw new IllegalArgumentException(String.format(
						"a function with the name %s is already registered and not a java function: %s",
						method.getName(), javaFunction));

				((JavaFunction) javaFunction).addSignature(method);
			}
		}
	}
}
