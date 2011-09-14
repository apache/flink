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

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class JavaFunction extends Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	public static final Log LOG = LogFactory.getLog(JavaFunction.class);

	private transient Map<MethodSignature, Method> cachedSignatures = new HashMap<MethodSignature, Method>();

	private transient Map<MethodSignature, Method> originalSignatures = new HashMap<MethodSignature, Method>();

	public JavaFunction(final String name) {
		super(name);
	}

	public void addSignature(final Method method) {
		final Class<?>[] parameterTypes = method.getParameterTypes();
		MethodSignature signature;
		if (parameterTypes.length == 1 && parameterTypes[0].isArray()
			&& JsonNode.class.isAssignableFrom(parameterTypes[0].getComponentType()))
			signature = new ArraySignature(parameterTypes[0]);
		else if (method.isVarArgs())
			signature = new VarArgSignature(parameterTypes);
		else
			signature = new MethodSignature(parameterTypes);
		this.originalSignatures.put(signature, method);
		// Cache flushing might be more intelligent in the future.
		// However, how often are method signatures actually added after first invocation?
		this.cachedSignatures.clear();
		this.cachedSignatures.putAll(this.originalSignatures);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final JsonNode[] params = this.getParams(node);
		final Class<?>[] paramTypes = this.getParamTypes(params);
		final Method method = this.findBestMethod(new MethodSignature(paramTypes));
		if (method == null)
			throw new EvaluationException(String.format("No method %s found for parameter types %s", this.getName(),
				Arrays.toString(paramTypes)));
		return this.invoke(method, params);
	}

	private Method findBestMethod(final MethodSignature signature) {
		Method method = this.cachedSignatures.get(signature);
		if (method != null)
			return method;

		int minDistance = MethodSignature.INCOMPATIBLE;
		boolean ambiguous = false;
		MethodSignature bestSignatureSoFar = null;
		for (final Entry<MethodSignature, Method> originalSignature : this.originalSignatures.entrySet()) {
			final int distance = originalSignature.getKey().getDistance(signature);
			if (distance < minDistance) {
				minDistance = distance;
				bestSignatureSoFar = originalSignature.getKey();
				ambiguous = false;
			} else if (distance == minDistance)
				ambiguous = true;
		}

		if (minDistance == MethodSignature.INCOMPATIBLE)
			return null;

		if (ambiguous && LOG.isWarnEnabled())
			this.warnForAmbiguity(signature, minDistance);

		method = minDistance == MethodSignature.INCOMPATIBLE ? null : this.originalSignatures.get(bestSignatureSoFar);
		this.cachedSignatures.put(bestSignatureSoFar, method);
		return method;
	}

	private JsonNode[] getParams(final JsonNode node) {
		JsonNode[] params;
		if (node instanceof CompactArrayNode) {
			params = new JsonNode[node.size()];

			for (int index = 0; index < params.length; index++)
				params[index] = node.get(index);
		} else
			params = new JsonNode[] { node };
		return params;
	}

	private Class<?>[] getParamTypes(final JsonNode[] params) {
		final Class<?>[] paramTypes = new Class<?>[params.length];
		for (int index = 0; index < paramTypes.length; index++)
			paramTypes[index] = params[index].getClass();
		return paramTypes;
	}

	public Collection<MethodSignature> getSignatures() {
		return this.originalSignatures.keySet();
	}

	public JsonNode invoke(final Method method, final JsonNode[] paramNodes) {
		try {
			Object[] params = paramNodes;
			if (method.isVarArgs()) {
				final Class<?>[] parameterTypes = method.getParameterTypes();
				final int varArgIndex = parameterTypes.length - 1;
				final int varArgCount = paramNodes.length - varArgIndex;
				final Object vararg = Array.newInstance(parameterTypes[varArgIndex].getComponentType(), varArgCount);
				for (int index = 0; index < varArgCount; index++)
					Array.set(vararg, index, paramNodes[varArgIndex + index]);

				params = new Object[parameterTypes.length];
				System.arraycopy(paramNodes, 0, params, 0, varArgIndex);
				params[varArgIndex] = vararg;
			} else if (method.getParameterTypes().length == 1 && params.length != 1)
				params = new Object[] { params };
			return (JsonNode) method.invoke(null, params);
		} catch (final Exception e) {
			throw new EvaluationException("Cannot invoke " + this.getName() + " with " + Arrays.toString(paramNodes), e);
		}
	}

	private void readObject(final ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		final int size = ois.readInt();
		this.cachedSignatures = new HashMap<MethodSignature, Method>();
		this.originalSignatures = new HashMap<MethodSignature, Method>();
		for (int index = 0; index < size; index++)
			try {
				this.originalSignatures.put((MethodSignature) ois.readObject(),
					((Class<?>) ois.readObject()).getDeclaredMethod(this.getName(), (Class<?>[]) ois.readObject()));
			} catch (final NoSuchMethodException e) {
				throw new EvaluationException("Cannot find registered java function " + this.getName(), e);
			}
	}

	private void warnForAmbiguity(final MethodSignature signature, final int minDistance) {
		final List<MethodSignature> ambigiousSignatures = new ArrayList<MethodSignature>();

		for (final Entry<MethodSignature, Method> originalSignature : this.originalSignatures.entrySet()) {
			final int distance = originalSignature.getKey().getDistance(signature);
			if (distance == minDistance)
				ambigiousSignatures.add(originalSignature.getKey());
		}

		LOG.warn(String.format("multiple matching signatures found for the method %s and parameters types %s: %s",
			this.getName(), Arrays.toString(signature.getParameterTypes()), ambigiousSignatures));
	}

	private void writeObject(final ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		oos.writeInt(this.originalSignatures.size());
		for (final Entry<MethodSignature, Method> entry : this.originalSignatures.entrySet()) {
			oos.writeObject(entry.getKey());
			oos.writeObject(entry.getValue().getDeclaringClass());
			oos.writeObject(entry.getValue().getParameterTypes());
		}
	}
}
