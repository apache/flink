package eu.stratosphere.sopremo.function;

import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Signature that takes an array of types as its only parameter.
 * 
 * @author Arvid Heise
 */
public class ArraySignature extends MethodSignature {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6152757876042585453L;

	/**
	 * Initializes ArraySignature with the given array parameter type.
	 * 
	 * @param arrayType
	 *        the array parameter type
	 */
	public ArraySignature(Class<?> arrayType) {
		super(new Class<?>[] { arrayType });
		if (!arrayType.isArray())
			throw new IllegalArgumentException();
	}

	/**
	 * Returns the sum of the distance of all types of the actual signature to the array component type of this
	 * signature.
	 * 
	 * @return the distance or {@link MethodSignature#INCOMPATIBLE}
	 */
	@Override
	public int getDistance(MethodSignature actualSignature) {
		Class<?>[] actualParamTypes = actualSignature.getParameterTypes();
		if (actualParamTypes.length == 0)
			return 1;

		Class<?> componentType = this.getParameterTypes()[0].getComponentType();
		if (actualParamTypes.length == 1 && actualParamTypes[0].isArray()
			&& this.getParameterTypes()[0].isAssignableFrom(actualParamTypes[0]))
			return ReflectUtil.getDistance(componentType, actualParamTypes[0].getComponentType()) + 1;

		int distance = 1;
		for (int index = 0; index < actualParamTypes.length; index++) {
			if (!componentType.isAssignableFrom(actualParamTypes[index]))
				return INCOMPATIBLE;
			distance += ReflectUtil.getDistance(componentType, actualParamTypes[index]);
		}

		return distance;
	}
}