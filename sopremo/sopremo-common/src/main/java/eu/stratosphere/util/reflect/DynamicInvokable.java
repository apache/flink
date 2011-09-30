package eu.stratosphere.util.reflect;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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

public abstract class DynamicInvokable<MemberType extends Member, DeclaringType, ReturnType> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 715358230985689455L;

	public static final Log LOG = LogFactory.getLog(DynamicInvokable.class);

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
					this.findMember((Class<DeclaringType>) ois.readObject(), (Class<?>[]) ois.readObject()));
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
			oos.writeObject(this.getParameterTypes(entry.getValue()));
		}
	}

	public String getName() {
		return this.name;
	}

	public DynamicInvokable(String name) {
		this.name = name;
	}

	public void addSignature(final MemberType member) {
		final Class<?>[] parameterTypes = this.getParameterTypes(member);
		Signature signature;
		/*
		 * if (parameterTypes.length == 1 && parameterTypes[0].isArray())
		 * signature = new ArraySignature(parameterTypes[0]);
		 * else
		 */
		if (this.isVarargs(member))
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

	private Signature findBestSignature(final Signature signature) {
		MemberType member = this.cachedSignatures.get(signature);
		if (member != null)
			return signature;

		int minDistance = Integer.MAX_VALUE;
		boolean ambiguous = false;
		Signature bestSignatureSoFar = null;
		for (final Entry<Signature, MemberType> originalSignature : this.originalSignatures.entrySet()) {
			final int distance = originalSignature.getKey().getDistance(signature);
			if (distance < 0)
				continue;
			
			if (distance < minDistance) {
				minDistance = distance;
				bestSignatureSoFar = originalSignature.getKey();
				ambiguous = false;
			} else if (distance == minDistance)
				ambiguous = true;
		}

		if (minDistance == Integer.MAX_VALUE)
			return null;

		if (ambiguous && LOG.isWarnEnabled())
			this.warnForAmbiguity(signature, minDistance);

		member = minDistance == Signature.INCOMPATIBLE ? null : this.originalSignatures.get(bestSignatureSoFar);
		this.cachedSignatures.put(bestSignatureSoFar, member);
		return bestSignatureSoFar;
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
		final Signature signature = this.findBestSignature(new Signature(paramTypes));
		if (signature == null)
			throw new EvaluationException(String.format("No method %s found for parameter types %s", this.getName(),
				Arrays.toString(paramTypes)));
		return this.invokeSignature(signature, context, params);
	}

	public abstract Class<ReturnType> getReturnType();

	public boolean isInvokableFor(Object... params) {
		final Class<?>[] paramTypes = this.getParamTypes(params);
		return this.findBestSignature(new Signature(paramTypes)) != null;
	}

	public ReturnType invokeSignature(final Signature signature, final Object context, final Object... paramNodes) {
		try {
			return this.invokeDirectly(this.cachedSignatures.get(signature), context,
				signature.adjustParameters(paramNodes));
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