package eu.stratosphere.sopremo.function;

import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Signature with a number of fixed arguments followed by a variable number of arguments of a specific type.
 * 
 * @author Arvid Heise
 */
public class VarArgSignature extends MethodSignature {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3584806055121932031L;

	/**
	 * Initializes VarArgSignature with the given argument types. In an actual invocation of the associated method, the
	 * may occur 0 to n instances of the last parameter type.
	 * 
	 * @param parameterTypes
	 *        the parameter types
	 */
	public VarArgSignature(final Class<?>[] parameterTypes) {
		super(parameterTypes);
	}

	@Override
	public int getDistance(final MethodSignature actualSignature) {
		final Class<?>[] actualParamTypes = actualSignature.getParameterTypes();
		final int nonVarArgs = this.getParameterTypes().length - 1;
		if (nonVarArgs > actualParamTypes.length)
			return INCOMPATIBLE;

		int distance = 0;
		for (int index = 0; index < nonVarArgs; index++) {
			if (!this.getParameterTypes()[index].isAssignableFrom(actualParamTypes[index]))
				return INCOMPATIBLE;
			distance += ReflectUtil.getDistance(this.getParameterTypes()[index], actualParamTypes[index]);
		}

		if (nonVarArgs < actualParamTypes.length) {
			final Class<?> varargType = this.getParameterTypes()[nonVarArgs].getComponentType();
			for (int index = nonVarArgs; index < actualParamTypes.length; index++) {
				if (!varargType.isAssignableFrom(actualParamTypes[index]))
					return INCOMPATIBLE;
				distance += ReflectUtil.getDistance(varargType, actualParamTypes[index]) + 1;
			}
		}

		return distance;
	}
}