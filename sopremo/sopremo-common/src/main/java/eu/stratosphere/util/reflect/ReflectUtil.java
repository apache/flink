package eu.stratosphere.util.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Convenience methods for reflective programming.
 * 
 * @author Arvid Heise
 */
public class ReflectUtil {
	private static Map<Class<?>, OverloadedContructor<?>> CACHED_CONSTRUCTORS = new HashMap<Class<?>, OverloadedContructor<?>>();

	@SuppressWarnings("serial")
	private final static Map<Class<?>, Class<?>> BoxingClasses = new IdentityHashMap<Class<?>, Class<?>>() {
		{
			this.put(java.lang.Boolean.TYPE, java.lang.Boolean.class);
			this.put(java.lang.Character.TYPE, java.lang.Character.class);
			this.put(java.lang.Byte.TYPE, java.lang.Byte.class);
			this.put(java.lang.Short.TYPE, java.lang.Short.class);
			this.put(java.lang.Integer.TYPE, java.lang.Integer.class);
			this.put(java.lang.Long.TYPE, java.lang.Long.class);
			this.put(java.lang.Float.TYPE, java.lang.Float.class);
			this.put(java.lang.Double.TYPE, java.lang.Double.class);
			this.put(java.lang.Void.TYPE, java.lang.Void.class);
		}
	};

	/**
	 * Returns the first annotation of the specified annotation type for the given type.<br>
	 * If no annotation is found for the type, the hierarchical ancestors are examined.
	 * 
	 * @param type
	 *        the type which might be annotated
	 * @param annotationType
	 *        the annotation type
	 * @return the annotation or null
	 */
	public static <A extends Annotation> A getAnnotation(final Class<?> type, final Class<A> annotationType) {
		A annotation = null;
		for (Class<?> t = type; annotation == null && t != null; t = t.getSuperclass())
			annotation = t.getAnnotation(annotationType);
		return annotation;
	}

	/**
	 * Returns the boxing class for the given primitive type. A primitive type returns true for
	 * {@link Class#isPrimitive()}.
	 * 
	 * @param primitive
	 *        the primitive type
	 * @return the boxing class or null if the given class is not a primitive
	 */
	public static Class<?> getClassForPrimtive(final Class<?> primitive) {
		return BoxingClasses.get(primitive);
	}

	/**
	 * Returns the hierarchical distance between a class and a derived class. <br>
	 * For instance,
	 * <ul>
	 * <li><code>getDistance(Integer.class, Integer.class) == 0</code>
	 * <li><code>getDistance(Number.class, Integer.class) == 1</code>
	 * <li><code>getDistance(Object.class, Integer.class) == 2</code>
	 * <li><code>getDistance(Comparable.class, Integer.class) == 1</code>
	 * <li><code>getDistance(Serializable.class, Integer.class) == 2</code>.
	 * </ul>
	 * 
	 * @param superClass
	 *        the super class in the hierarchy
	 * @param subclass
	 *        the sub class of the hierarchy
	 * @return the minimum distance
	 */
	public static int getDistance(final Class<?> superClass, final Class<?> subclass) {
		if (superClass == subclass)
			return 0;
		if (!superClass.isAssignableFrom(subclass))
			return Integer.MAX_VALUE;

		if (superClass.isInterface()) {
			final Class<?>[] interfaces = subclass.getInterfaces();
			int minDistance = Integer.MAX_VALUE;
			for (final Class<?> xface : interfaces)
				if (xface == superClass) {
					minDistance = 1;
					break;
				} else if (superClass.isAssignableFrom(xface))
					minDistance = Math.min(minDistance, getDistance(superClass, xface));
			return minDistance;
		}

		int distance = 1;
		for (Class<?> klazz = subclass; superClass != klazz; distance++)
			klazz = klazz.getSuperclass();
		return distance;
	}

	/**
	 * Dynamically retrieves the value of the specified field of an object.
	 * 
	 * @param object
	 *        the object to invoke on
	 * @param fieldName
	 *        the name of the field
	 * @return the value of the field
	 */
	public static Object getFieldValue(final Object object, final String fieldName) {
		final Class<? extends Object> type = object.getClass();
		try {
			final Field field = type.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (final Exception e) {
			throw new IllegalArgumentException(String.format("Could not get field value %s for type %s", fieldName,
				type), e);
		}
	}

	/**
	 * Dynamically retrieves the static value of the specified field of a type.
	 * 
	 * @param type
	 *        the type to invoke on
	 * @param fieldName
	 *        the name of the field
	 * @return the value of the field
	 */
	public static Object getStaticValue(final Class<?> type, final String fieldName) {
		try {
			final Field field = type.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(null);
		} catch (final Exception e) {
			throw new IllegalArgumentException(String.format("Could not get field value %s for type %s", fieldName,
				type), e);
		}
	}

	/**
	 * Checks dynamically whether the object has the specified function, which takes the given parameters.
	 * 
	 * @param object
	 *        the object to invoke on
	 * @param function
	 *        the function to call
	 * @param params
	 *        the parameters of the function
	 * @return true if such a method exists
	 */
	public static boolean hasFunction(final Object object, final String function, final Object... params) {
		return OverloadedMethod.valueOf(object.getClass(), function).isInvokableFor(object, params);
	}

	/**
	 * Dynamically invokes the specified function on an object with the given parameters.
	 * 
	 * @param object
	 *        the object to invoke on
	 * @param function
	 *        the function to call
	 * @param params
	 *        the parameters of the function
	 * @return the result of the invocation
	 */
	public static Object invoke(final Object object, final String function, final Object... params) {
		return OverloadedMethod.valueOf(object.getClass(), function).invoke(object, params);
	}

	/**
	 * Returns true if the given type has an accessible default constructor.<br>
	 * Note: this method is thread-safe
	 * 
	 * @param type
	 *        the type to check
	 * @return true if it has an accessible default constructor.
	 */
	public static boolean isInstantiable(final Class<?> type) {
		return OverloadedContructor.valueOf(type).isInvokableFor();
	}

	/**
	 * Returns true if both types are the same or represent the same primitive or boxing type.
	 * 
	 * @param type1
	 *        the first type
	 * @param type2
	 *        the second type
	 * @return the boxing class or null if the given class is not a primitive
	 */
	public static boolean isSameTypeOrPrimitive(final Class<?> type1, final Class<?> type2) {
		final Class<?> t1 = type1.isPrimitive() ? getClassForPrimtive(type1) : type1;
		final Class<?> t2 = type2.isPrimitive() ? getClassForPrimtive(type2) : type2;
		return t1 == t2;
	}

	/**
	 * Creates a new instance of the given type by invoking the default constructor. If the default constructor is not
	 * public, the method will try to
	 * gain access through {@link Constructor#setAccessible(boolean)}. <br>
	 * <br>
	 * Note: this method is not thread-safe
	 * 
	 * @param <T>
	 *        the type to instantiate
	 * @param type
	 *        the type to instantiate
	 * @return the created instance
	 * @throws IllegalArgumentException
	 *         if the type has no accessible default constructor or an exception occurred during the invocation:
	 *         possible causes are {@link NoSuchMethodException}, {@link InstantiationException} ,
	 *         {@link IllegalAccessException}, * {@link InvocationTargetException}
	 */
	public static <T> T newInstance(final Class<T> type) throws IllegalArgumentException {
		return OverloadedContructor.valueOf(type).invoke();
	}

	/**
	 * Creates a new instance of the given type by invoking the best public constructor for the given parameter.<br>
	 * If there are multiple compatible constructors, the most specific one is chosen. <br>
	 * If there are several constructors with the same degree of specify, an Exception is thrown. <br>
	 * Note: this method is thread-safe
	 * 
	 * @param <T>
	 *        the type to instantiate
	 * @param type
	 *        the type to instantiate
	 * @param params
	 *        The constructors parameters.
	 * @return the created instance
	 * @throws IllegalArgumentException
	 *         if the type has 0 or more than 2 matching constructors or an exception occurred during the invocation:
	 *         possible causes are {@link NoSuchMethodException}, {@link InstantiationException} ,
	 *         {@link IllegalAccessException}, {@link InvocationTargetException}
	 */
	public static <T> T newInstance(final Class<T> type, final Object... params) throws IllegalArgumentException {
		return OverloadedContructor.valueOf(type).invoke(params);
	}
}
