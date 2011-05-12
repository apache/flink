package eu.stratosphere.sopremo;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;

import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.reflect.ReflectUtil;

public class JavaFunction extends Function {
	public static final Log LOG = LogFactory.getLog(JavaFunction.class);

	private static class Signature {
		private Class<?>[] parameterTypes;

		public Signature(Class<?>[] parameterTypes) {
			this.parameterTypes = parameterTypes;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(parameterTypes);
			return result;
		}

		public Class<?>[] getParameterTypes() {
			return parameterTypes;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Signature other = (Signature) obj;
			return Arrays.equals(parameterTypes, other.parameterTypes);
		}

		public int getDistance(Signature actualSignature, Method method) {
			Class<?>[] actualParamTypes = actualSignature.parameterTypes;
			int nonVarArgs = parameterTypes.length;
			if (method.isVarArgs())
				nonVarArgs--;
			if (!method.isVarArgs() && nonVarArgs != actualParamTypes.length)
				return NO_MATCH;
			if (method.isVarArgs() && nonVarArgs > actualParamTypes.length)
				return NO_MATCH;

			int distance = 0;
			for (int index = 0; index < nonVarArgs; index++) {
				if (!parameterTypes[index].isAssignableFrom(actualParamTypes[index]))
					return NO_MATCH;
				distance += ReflectUtil.getDistance(parameterTypes[index], actualParamTypes[index]);
			}

			if (method.isVarArgs() && nonVarArgs < actualParamTypes.length) {
				Class<?> varargType = parameterTypes[nonVarArgs].getComponentType();
				for (int index = nonVarArgs; index < actualParamTypes.length; index++) {
					if (!varargType.isAssignableFrom(actualParamTypes[index]))
						return NO_MATCH;
					distance += ReflectUtil.getDistance(varargType, actualParamTypes[index]) + 1;
				}
			}

			return distance;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("(").append(Arrays.toString(parameterTypes)).append(")");
			return builder.toString();
		}
	}

	private static final int NO_MATCH = Integer.MAX_VALUE;

	private transient Map<Signature, Method> cachedSignatures = new HashMap<Signature, Method>();

	private Map<Signature, Method> originalSignatures = new HashMap<Signature, Method>();

	public JavaFunction(String name) {
		super(name);
	}

	public Collection<Signature> getSignatures() {
		return originalSignatures.keySet();
	}

	public void addSignature(Method method) {
		originalSignatures.put(new Signature(method.getParameterTypes()), method);
		// might be more intelligent in the future
		// how often are method signatures actually added after first invocation?
		cachedSignatures.clear();
		cachedSignatures.putAll(originalSignatures);
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		JsonNode[] params = getParams(node);
		Class<?>[] paramTypes = getParamTypes(params);
		Method method = findBestMethod(new Signature(paramTypes));
		if (method == null)
			throw new EvaluationException(String.format("No method %s found for parameter types %s", getName(),
				Arrays.toString(paramTypes)));
		return invoke(method, params);
	}

	public JsonNode invoke(Method method, JsonNode[] paramNodes) {
		try {
			Object[] params = paramNodes;
			if (method.isVarArgs()) {
				Class<?>[] parameterTypes = method.getParameterTypes();
				int varArgIndex = parameterTypes.length - 1;
				int varArgCount = paramNodes.length - varArgIndex;
				Object vararg = Array.newInstance(parameterTypes[varArgIndex].getComponentType(), varArgCount);
				for (int index = 0; index < varArgCount; index++)
					Array.set(vararg, index, paramNodes[varArgIndex + index]);
				
				params = new Object[parameterTypes.length];
				System.arraycopy(paramNodes, 0, params, 0, varArgIndex);
				params[varArgIndex] = vararg;
			}
			return (JsonNode) method.invoke(null, params);
		} catch (Exception e) {
			throw new EvaluationException("Cannot invoke " + getName() + " with " + Arrays.toString(paramNodes), e);
		}
	}

	private Class<?>[] getParamTypes(JsonNode[] params) {
		Class<?>[] paramTypes = new Class<?>[params.length];
		for (int index = 0; index < paramTypes.length; index++)
			paramTypes[index] = params[index].getClass();
		return paramTypes;
	}

	private JsonNode[] getParams(JsonNode node) {
		JsonNode[] params;
		if (node instanceof CompactArrayNode) {
			params = new JsonNode[node.size()];

			for (int index = 0; index < params.length; index++)
				params[index] = node.get(index);
		} else
			params = new JsonNode[] { node };
		return params;
	}

	private Method findBestMethod(Signature signature) {
		Method method = cachedSignatures.get(signature);
		if (method != null)
			return method;

		int minDistance = NO_MATCH;
		boolean ambiguous = false;
		Signature bestSignatureSoFar = null;
		for (Entry<Signature, Method> originalSignature : originalSignatures.entrySet()) {
			int distance = originalSignature.getKey().getDistance(signature, originalSignature.getValue());
			if (distance < minDistance) {
				minDistance = distance;
				bestSignatureSoFar = originalSignature.getKey();
				ambiguous = false;
			} else if (distance == minDistance)
				ambiguous = true;
		}

		if (minDistance == NO_MATCH)
			return null;

		if (ambiguous && LOG.isWarnEnabled())
			warnForAmbiguity(signature, minDistance);

		method = minDistance == NO_MATCH ? null : cachedSignatures.get(bestSignatureSoFar);
		cachedSignatures.put(bestSignatureSoFar, method);
		return method;
	}

	private void warnForAmbiguity(Signature signature, int minDistance) {
		List<Signature> ambigiousSignatures = new ArrayList<Signature>();

		for (Entry<Signature, Method> originalSignature : originalSignatures.entrySet()) {
			int distance = originalSignature.getKey().getDistance(signature, originalSignature.getValue());
			if (distance == minDistance)
				ambigiousSignatures.add(originalSignature.getKey());
		}

		LOG.warn(String.format("multiple matching signatures found for the method %s and parameters types %s: %s",
			getName(), Arrays.toString(signature.getParameterTypes()), ambigiousSignatures));
	}
}
