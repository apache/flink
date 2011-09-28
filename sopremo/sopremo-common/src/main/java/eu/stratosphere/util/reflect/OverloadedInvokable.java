package eu.stratosphere.util.reflect;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.sopremo.EvaluationException;

public abstract class OverloadedInvokable<MemberType extends Member, DeclaringType, ReturnType> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 715358230985689455L;

	public static final Log LOG = LogFactory.getLog(OverloadedInvokable.class);

	private transient Map<Signature, MemberType> cachedSignatures = new HashMap<Signature, MemberType>();

	private transient Map<Signature, MemberType> originalSignatures = new HashMap<Signature, MemberType>();

	private final String name;

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		final int size = ois.readInt();
		this.cachedSignatures = new HashMap<Signature, MemberType>();
		this.originalSignatures = new HashMap<Signature, MemberType>();
		for (int index = 0; index < size; index++)
			try {
				this.originalSignatures.put((Signature) ois.readObject(),
					findMember((Class<DeclaringType>) ois.readObject(), (Class<?>[]) ois.readObject()));
			} catch (final NoSuchMethodException e) {
				throw new EvaluationException("Cannot find registered java function " + this.getName(), e);
			}
	}

	protected abstract MemberType findMember(Class<DeclaringType> clazz, Class<?>[] parameterTypes)
			throws NoSuchMethodException;

	private void writeObject(final ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		oos.writeInt(this.originalSignatures.size());
		for (final Entry<Signature, MemberType> entry : this.originalSignatures.entrySet()) {
			oos.writeObject(entry.getKey());
			oos.writeObject(entry.getValue().getDeclaringClass());
			oos.writeObject(getParameterTypes(entry.getValue()));
		}
	}

	public String getName() {
		return this.name;
	}

	public OverloadedInvokable(String name) {
		this.name = name;
	}

	public void addSignature(final MemberType member) {
		final Class<?>[] parameterTypes = this.getParameterTypes(member);
		Signature signature;
		if (parameterTypes.length == 1 && parameterTypes[0].isArray())
			signature = new ArraySignature(parameterTypes[0]);
		else if (this.isVarargs(member))
			signature = new VarArgSignature(parameterTypes);
		else
			signature = new Signature(parameterTypes);
		this.originalSignatures.put(signature, member);
		// Cache flushing might be more intelligent in the future.
		// However, how often are method signatures actually added after first invocation?
		this.cachedSignatures.clear();
		this.cachedSignatures.putAll(this.originalSignatures);
	}

	protected abstract boolean isVarargs(final MemberType member);

	protected abstract Class<?>[] getParameterTypes(final MemberType member);

	private MemberType findBestOverload(final Signature signature) {
		MemberType member = this.cachedSignatures.get(signature);
		if (member != null)
			return member;

		int minDistance = Signature.INCOMPATIBLE;
		boolean ambiguous = false;
		Signature bestSignatureSoFar = null;
		for (final Entry<Signature, MemberType> originalSignature : this.originalSignatures.entrySet()) {
			final int distance = originalSignature.getKey().getDistance(signature);
			if (distance < minDistance) {
				minDistance = distance;
				bestSignatureSoFar = originalSignature.getKey();
				ambiguous = false;
			} else if (distance == minDistance)
				ambiguous = true;
		}

		if (minDistance == Signature.INCOMPATIBLE)
			return null;

		if (ambiguous && LOG.isWarnEnabled())
			this.warnForAmbiguity(signature, minDistance);

		member = minDistance == Signature.INCOMPATIBLE ? null : this.originalSignatures.get(bestSignatureSoFar);
		this.cachedSignatures.put(bestSignatureSoFar, member);
		return member;
	}

	private void warnForAmbiguity(final Signature signature, final int minDistance) {
		final List<Signature> ambigiousSignatures = new ArrayList<Signature>();

		for (final Entry<Signature, MemberType> originalSignature : this.originalSignatures.entrySet()) {
			final int distance = originalSignature.getKey().getDistance(signature);
			if (distance == minDistance)
				ambigiousSignatures.add(originalSignature.getKey());
		}

		LOG.warn(String.format("multiple matching signatures found for the member %s and parameters types %s: %s",
			this.getName(), Arrays.toString(signature.getParameterTypes()), ambigiousSignatures));
	}

	public ReturnType invoke(final Object context, final Object... params) {
		final Class<?>[] paramTypes = this.getParamTypes(params);
		final MemberType member = this.findBestOverload(new Signature(paramTypes));
		if (member == null)
			throw new EvaluationException(String.format("No method %s found for parameter types %s", this.getName(),
				Arrays.toString(paramTypes)));
		return this.invokeMember(member, context, params);
	}

	public abstract Class<ReturnType> getReturnType();

	public boolean isInvokableFor(Object... params) {
		final Class<?>[] paramTypes = this.getParamTypes(params);
		final MemberType member = this.findBestOverload(new Signature(paramTypes));
		return member != null;
	}

	public ReturnType invokeMember(final MemberType method, final Object context, final Object... paramNodes) {
		try {
			Object[] params = paramNodes;
			final Class<?>[] parameterTypes = this.getParameterTypes(method);
			if (this.isVarargs(method)) {
				final int varArgIndex = parameterTypes.length - 1;
				final int varArgCount = paramNodes.length - varArgIndex;
				final Object vararg = Array.newInstance(parameterTypes[varArgIndex].getComponentType(), varArgCount);
				for (int index = 0; index < varArgCount; index++)
					Array.set(vararg, index, paramNodes[varArgIndex + index]);

				params = new Object[parameterTypes.length];
				System.arraycopy(paramNodes, 0, params, 0, varArgIndex);
				params[varArgIndex] = vararg;
			} else if (parameterTypes.length == 1 && params.length != 1)
				params = new Object[] { params };
			return this.invokeDirectly(method, context, params);
		} catch (final Exception e) {
			throw new EvaluationException("Cannot invoke " + this.getName() + " with " + Arrays.toString(paramNodes), e);
		}
	}

	protected abstract ReturnType invokeDirectly(final MemberType member, final Object context, Object[] params)
			throws IllegalAccessException, InvocationTargetException, IllegalArgumentException, InstantiationException;

	private Class<?>[] getParamTypes(final Object[] params) {
		final Class<?>[] paramTypes = new Class<?>[params.length];
		for (int index = 0; index < paramTypes.length; index++)
			paramTypes[index] = params[index].getClass();
		return paramTypes;
	}

	public Collection<Signature> getSignatures() {
		return this.originalSignatures.keySet();
	}

}