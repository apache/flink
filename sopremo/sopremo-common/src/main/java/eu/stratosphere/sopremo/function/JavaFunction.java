package eu.stratosphere.sopremo.function;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

import eu.stratosphere.reflect.ReflectUtil;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.SerializableSopremoType;

public class JavaFunction extends Function {
	public static final Log LOG = LogFactory.getLog(JavaFunction.class);

	private static class Signature implements SerializableSopremoType {
		protected final Class<?>[] parameterTypes;

		public Signature(Class<?>[] parameterTypes) {
			this.parameterTypes = parameterTypes;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(this.parameterTypes);
			return result;
		}

		public Class<?>[] getParameterTypes() {
			return this.parameterTypes;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			Signature other = (Signature) obj;
			return Arrays.equals(this.parameterTypes, other.parameterTypes);
		}

		public int getDistance(Signature actualSignature) {
			Class<?>[] actualParamTypes = actualSignature.parameterTypes;
			if (this.parameterTypes.length != actualParamTypes.length)
				return NO_MATCH;

			int distance = 0;
			for (int index = 0; index < this.parameterTypes.length; index++) {
				if (!this.parameterTypes[index].isAssignableFrom(actualParamTypes[index]))
					return NO_MATCH;
				distance += ReflectUtil.getDistance(this.parameterTypes[index], actualParamTypes[index]);
			}

			return distance;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("(").append(Arrays.toString(this.parameterTypes)).append(")");
			return builder.toString();
		}
	}

	private static class ArraySignature extends Signature {
		public ArraySignature(Class<?>[] parameterTypes) {
			super(parameterTypes);
		}

		@Override
		public int getDistance(Signature actualSignature) {
			Class<?>[] actualParamTypes = actualSignature.parameterTypes;
			if (actualParamTypes.length == 0)
				return 1;

			Class<?> componentType = this.parameterTypes[0].getComponentType();
			if (actualParamTypes.length == 1 && actualParamTypes[0].isArray()
				&& this.parameterTypes[0].isAssignableFrom(actualParamTypes[0]))
				return ReflectUtil.getDistance(componentType, actualParamTypes[0].getComponentType()) + 1;

			int distance = 1;
			for (int index = 0; index < actualParamTypes.length; index++) {
				if (!componentType.isAssignableFrom(actualParamTypes[index]))
					return NO_MATCH;
				distance += ReflectUtil.getDistance(componentType, actualParamTypes[index]);
			}

			return distance;
		}
	}

	private static class VarArgSignature extends Signature {
		public VarArgSignature(Class<?>[] parameterTypes) {
			super(parameterTypes);
		}

		@Override
		public int getDistance(Signature actualSignature) {
			Class<?>[] actualParamTypes = actualSignature.parameterTypes;
			int nonVarArgs = this.parameterTypes.length - 1;
			if (nonVarArgs > actualParamTypes.length)
				return NO_MATCH;

			int distance = 0;
			for (int index = 0; index < nonVarArgs; index++) {
				if (!this.parameterTypes[index].isAssignableFrom(actualParamTypes[index]))
					return NO_MATCH;
				distance += ReflectUtil.getDistance(this.parameterTypes[index], actualParamTypes[index]);
			}

			if (nonVarArgs < actualParamTypes.length) {
				Class<?> varargType = this.parameterTypes[nonVarArgs].getComponentType();
				for (int index = nonVarArgs; index < actualParamTypes.length; index++) {
					if (!varargType.isAssignableFrom(actualParamTypes[index]))
						return NO_MATCH;
					distance += ReflectUtil.getDistance(varargType, actualParamTypes[index]) + 1;
				}
			}

			return distance;
		}
	}

	private static final int NO_MATCH = Integer.MAX_VALUE;

	private transient Map<Signature, Method> cachedSignatures = new HashMap<Signature, Method>();

	private transient Map<Signature, Method> originalSignatures = new HashMap<Signature, Method>();

	public JavaFunction(String name) {
		super(name);
	}

	public Collection<Signature> getSignatures() {
		return this.originalSignatures.keySet();
	}

	public void addSignature(Method method) {
		Class<?>[] parameterTypes = method.getParameterTypes();
		Signature signature;
		if (parameterTypes.length == 1 && parameterTypes[0].isArray()
			&& JsonNode.class.isAssignableFrom(parameterTypes[0].getComponentType()))
			signature = new ArraySignature(parameterTypes);
		else if (method.isVarArgs())
			signature = new VarArgSignature(parameterTypes);
		else
			signature = new Signature(parameterTypes);
		this.originalSignatures.put(signature, method);
		// Cache flushing might be more intelligent in the future.
		// However, how often are method signatures actually added after first invocation?
		this.cachedSignatures.clear();
		this.cachedSignatures.putAll(this.originalSignatures);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		oos.writeInt(this.originalSignatures.size());
		for (Entry<Signature, Method> entry : this.originalSignatures.entrySet()) {
			oos.writeObject(entry.getKey());
			oos.writeObject(entry.getValue().getDeclaringClass());
			oos.writeObject(entry.getValue().getParameterTypes());
		}
	}

	// assumes "static java.util.Date aDate;" declared
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		int size = ois.readInt();
		this.cachedSignatures = new HashMap<Signature, Method>();
		this.originalSignatures = new HashMap<Signature, Method>();
		for (int index = 0; index < size; index++)
			try {
				this.originalSignatures.put((Signature) ois.readObject(),
					((Class<?>) ois.readObject()).getDeclaredMethod(this.getName(), (Class<?>[]) ois.readObject()));
			} catch (NoSuchMethodException e) {
				throw new EvaluationException("Cannot find registered java function " + this.getName(), e);
			}
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		JsonNode[] params = this.getParams(node);
		Class<?>[] paramTypes = this.getParamTypes(params);
		Method method = this.findBestMethod(new Signature(paramTypes));
		if (method == null)
			throw new EvaluationException(String.format("No method %s found for parameter types %s", this.getName(),
				Arrays.toString(paramTypes)));
		return this.invoke(method, params);
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
			} else if(method.getParameterTypes().length == 1 && params.length != 1)
				params = new Object[] { params };
			return (JsonNode) method.invoke(null, params);
		} catch (Exception e) {
			throw new EvaluationException("Cannot invoke " + this.getName() + " with " + Arrays.toString(paramNodes), e);
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
		Method method = this.cachedSignatures.get(signature);
		if (method != null)
			return method;

		int minDistance = NO_MATCH;
		boolean ambiguous = false;
		Signature bestSignatureSoFar = null;
		for (Entry<Signature, Method> originalSignature : this.originalSignatures.entrySet()) {
			int distance = originalSignature.getKey().getDistance(signature);
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
			this.warnForAmbiguity(signature, minDistance);

		method = minDistance == NO_MATCH ? null : this.originalSignatures.get(bestSignatureSoFar);
		this.cachedSignatures.put(bestSignatureSoFar, method);
		return method;
	}

	private void warnForAmbiguity(Signature signature, int minDistance) {
		List<Signature> ambigiousSignatures = new ArrayList<Signature>();

		for (Entry<Signature, Method> originalSignature : this.originalSignatures.entrySet()) {
			int distance = originalSignature.getKey().getDistance(signature);
			if (distance == minDistance)
				ambigiousSignatures.add(originalSignature.getKey());
		}

		LOG.warn(String.format("multiple matching signatures found for the method %s and parameters types %s: %s",
			this.getName(), Arrays.toString(signature.getParameterTypes()), ambigiousSignatures));
	}
}
