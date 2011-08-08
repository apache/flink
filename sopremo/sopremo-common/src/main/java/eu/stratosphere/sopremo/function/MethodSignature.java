package eu.stratosphere.sopremo.function;

import java.util.Arrays;

import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * General signature of a method that handles exactly the specified argument types.
 * 
 * @author Arvid Heise
 */
public class MethodSignature implements SerializableSopremoType {
	/**
	 * Constant that is returned by {@link #getDistance(MethodSignature)} if the given actual signature is incompatible
	 * with the declared signature.
	 */
	protected static final int INCOMPATIBLE = Integer.MAX_VALUE;

	/**
	 * 
	 */
	private static final long serialVersionUID = -3618253913777961043L;

	private final Class<?>[] parameterTypes;

	/**
	 * Initializes MethodSignature with the given, declared parameter types.
	 * 
	 * @param parameterTypes
	 *        the parameter types
	 */
	public MethodSignature(final Class<?>[] parameterTypes) {
		this.parameterTypes = parameterTypes;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final MethodSignature other = (MethodSignature) obj;
		return Arrays.equals(this.parameterTypes, other.parameterTypes);
	}

	/**
	 * Returns the distance between this signature and the given signature by summing up the hierarchical distance
	 * between the actual and the declared type with {@link ReflectUtil#getDistance(Class, Class)}.
	 * 
	 * @param actualSignature
	 *        the actual signature that should have equal or more specific types than this declared signature
	 * @return the distance or {@link #INCOMPATIBLE}
	 */
	public int getDistance(final MethodSignature actualSignature) {
		final Class<?>[] actualParamTypes = actualSignature.parameterTypes;
		if (this.parameterTypes.length != actualParamTypes.length)
			return INCOMPATIBLE;

		int distance = 0;
		for (int index = 0; index < this.parameterTypes.length; index++) {
			if (!this.parameterTypes[index].isAssignableFrom(actualParamTypes[index]))
				return INCOMPATIBLE;
			distance += ReflectUtil.getDistance(this.parameterTypes[index], actualParamTypes[index]);
		}

		return distance;
	}

	/**
	 * Returns the declared parameter types of a method.
	 * 
	 * @return the parameter types
	 */
	public Class<?>[] getParameterTypes() {
		return this.parameterTypes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.parameterTypes);
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append("(").append(Arrays.toString(this.parameterTypes)).append(")");
		return builder.toString();
	}
}