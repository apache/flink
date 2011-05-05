package eu.stratosphere.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Convenience methods for reflective programming.
 * 
 * @author Arvid Heise
 */
public class ReflectUtil {
	private static Map<Class<?>, Constructor<?>> CACHED_DEFAULT_CONSTRUCTORS = new HashMap<Class<?>, Constructor<?>>();

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
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(Class<T> type) throws IllegalArgumentException {
		try {
			Constructor<T> constructor = (Constructor<T>) CACHED_DEFAULT_CONSTRUCTORS.get(type);
			if (constructor == null) {
				CACHED_DEFAULT_CONSTRUCTORS.put(type, constructor = type.getDeclaredConstructor());
				constructor.setAccessible(true);
			}
			return constructor.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not create an instance of type " + type, e);
		}
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
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(Class<T> type, Object... params) throws IllegalArgumentException {
		try {
			Map<Constructor<?>, Integer> candidateDistances = getCompatibleConstructors(type, params);

			if (candidateDistances.isEmpty())
				throw new IllegalArgumentException(String.format("no suitable constructor found in %s for %s", type,
					Arrays.toString(params)));

			if (candidateDistances.size() == 1)
				return (T) candidateDistances.keySet().iterator().next().newInstance(params);

			Constructor<?> bestConstructor = pickBest(candidateDistances);
			if (bestConstructor == null)
				throw new IllegalArgumentException(String.format(
					"more than one suitable constructor found in %s for %s", type, Arrays
						.toString(params)));
			return (T) bestConstructor.newInstance(params);
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not create an instance of type " + type, e);
		}
	}

	private static <T> T pickBest(Map<T, Integer> candidateDistances) {
		int minDistance = Integer.MAX_VALUE;
		int minCount = 0;
		T minConstructor = null;
		for (Entry<T, Integer> entry : candidateDistances.entrySet())
			if (entry.getValue() < minDistance) {
				minDistance = entry.getValue();
				minConstructor = entry.getKey();
				minCount = 1;
			} else if (entry.getValue() == minDistance)
				minCount++;

		return minCount == 1 ? minConstructor : null;
	}

	private static <T> Map<Constructor<?>, Integer> getCompatibleConstructors(Class<T> type, Object... params) {
		Constructor<?>[] constructors = type.getDeclaredConstructors();
		Map<Constructor<?>, Integer> candidateDistances = new HashMap<Constructor<?>, Integer>();
		for (Constructor<?> constructor : constructors) {
			int distance = 0;
			Class<?>[] parameterTypes = constructor.getParameterTypes();
			if (params.length != parameterTypes.length)
				continue;

			for (int index = 0; index < parameterTypes.length; index++) {
				if (!parameterTypes[index].isInstance(params[index])) {
					distance = Integer.MAX_VALUE;
					break;
				}

				if (params[index] != null)
					distance += getDistance(parameterTypes[index], params[index].getClass());
			}
			candidateDistances.put(constructor, distance);
		}
		return candidateDistances;
	}

	private static <T> Map<Method, Integer> getCompatibleMethods(Class<T> type, String name, Object... params) {
		Method[] methods = type.getDeclaredMethods();
		Map<Method, Integer> candidateDistances = new HashMap<Method, Integer>();
		for (Method method : methods) {
			if (!method.getName().equals(name))
				continue;

			int distance = 0;
			Class<?>[] parameterTypes = method.getParameterTypes();
			if (params.length != parameterTypes.length)
				continue;

			for (int index = 0; index < parameterTypes.length; index++) {
				if (!parameterTypes[index].isInstance(params[index])) {
					distance = Integer.MAX_VALUE;
					break;
				}

				if (params[index] != null)
					distance += getDistance(parameterTypes[index], params[index].getClass());
			}
			candidateDistances.put(method, distance);
		}
		if (type.getSuperclass() != null)
			candidateDistances.putAll(getCompatibleMethods(type.getSuperclass(), name, params));
		return candidateDistances;
	}

	private static int getDistance(Class<?> superClass, Class<?> subclass) {
		if (superClass == subclass)
			return 0;
		if (!superClass.isAssignableFrom(subclass))
			return Integer.MAX_VALUE;

		if (superClass.isInterface()) {
			Class<?>[] interfaces = subclass.getInterfaces();
			int minDistance = Integer.MAX_VALUE;
			for (Class<?> xface : interfaces)
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

	private static List<Type> getHierarchy(Class<?> superClass, Class<?> subclass) {
		if (!superClass.isAssignableFrom(subclass))
			throw new IllegalArgumentException();

		List<Type> hierarchy = new ArrayList<Type>();
		if (superClass == subclass)
			return hierarchy;

		if (superClass.isInterface()) {
			Type[] interfaces = subclass.getGenericInterfaces();
			int minDistance = Integer.MAX_VALUE;
			for (Type xface : interfaces) {

				Class<?> type = (Class<?>) (xface instanceof Class ? xface : ((ParameterizedType) xface).getRawType());
				if (type == superClass) {
					hierarchy.clear();
					hierarchy.add(xface);
					break;
				}

				if (superClass.isAssignableFrom(type)) {
					List<Type> partialHierarchy = getHierarchy(superClass, type);
					if (partialHierarchy.size() + 1 < minDistance) {
						hierarchy = partialHierarchy;
						hierarchy.add(0, xface);
						minDistance = hierarchy.size();
					}
				}
			}
			if (hierarchy.isEmpty()) {
				hierarchy.add(subclass.getGenericSuperclass());
				hierarchy.addAll(getHierarchy(superClass, subclass.getSuperclass()));
			}
			return hierarchy;
		}

		Type clazz = subclass;
		do {
			hierarchy.add(clazz);
			Class<?> type = (Class<?>) (clazz instanceof Class ? clazz : ((ParameterizedType) clazz).getRawType());
			clazz = type.getGenericSuperclass();
		} while (superClass != clazz);
		return hierarchy;
	}

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
	 * Returns the boxing class for the given primitive type. A primitive type returns true for
	 * {@link Class#isPrimitive()}.
	 * 
	 * @param primitive
	 *        the primitive type
	 * @return the boxing class or null if the given class is not a primitive
	 */
	public static Class<?> getClassForPrimtive(Class<?> primitive) {
		return BoxingClasses.get(primitive);
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
	public static boolean isSameTypeOrPrimitive(Class<?> type1, Class<?> type2) {
		final Class<?> t1 = type1.isPrimitive() ? getClassForPrimtive(type1) : type1;
		final Class<?> t2 = type2.isPrimitive() ? getClassForPrimtive(type2) : type2;
		return t1 == t2;
	}

	/**
	 * Returns true if the given type has an accessible default constructor.<br>
	 * Note: this method is thread-safe
	 * 
	 * @param type
	 *        the type to check
	 * @return true if it has an accessible default constructor.
	 */
	public static Boolean isInstantiable(Class<?> type) {
		synchronized (CACHED_DEFAULT_CONSTRUCTORS) {
			try {
				Constructor<?> constructor = CACHED_DEFAULT_CONSTRUCTORS.get(type);
				if (constructor == null) {
					CACHED_DEFAULT_CONSTRUCTORS.put(type, constructor = type.getDeclaredConstructor());
					constructor.setAccessible(true);
				}
				return true;
			} catch (Exception e) {
				return false;
			}
		}
	}

	/**
	 * Returns the static bounds for the given type. <br>
	 * A static bound occurs if a subclass specifies the type parameter of the superclass explicitly.<br>
	 * Example: <code>class IntList extends ArrayList&lt;Integer&gt; {}</code> would result in a
	 * <code>BoundType.of(Integer.class)</code>.
	 * 
	 * @param parameterizedType
	 *        the type to examine
	 * @return all static bounds
	 */
	public static BoundType[] getStaticBoundTypes(ParameterizedType parameterizedType) {
		List<BoundType> boundedTypes = new ArrayList<BoundType>();
		for (Type type : parameterizedType.getActualTypeArguments())
			if (type instanceof Class<?>)
				boundedTypes.add(new BoundType((Class<?>) type));
			else if (type instanceof ParameterizedType)
				boundedTypes.add(new BoundType((ParameterizedType) type));

		Class<?> rawType = (Class<?>) parameterizedType.getRawType();
		if (rawType.getGenericSuperclass() instanceof ParameterizedType)
			boundedTypes.addAll(0,
				Arrays.asList(getStaticBoundTypes((ParameterizedType) rawType.getGenericSuperclass())));

		return boundedTypes.toArray(new BoundType[boundedTypes.size()]);
	}

	/**
	 * Returns the static bounds for the given type. <br>
	 * A static bound occurs if a subclass specifies the type parameter of the superclass explicitly.<br>
	 * Example: <code>class IntList extends ArrayList&lt;Integer&gt; {}</code> would result in a
	 * <code>BoundType.of(Integer.class)</code>.
	 * 
	 * @param klass
	 *        the type to examine
	 * @return all static bounds
	 */
	public static BoundType[] getStaticBoundTypes(Class<?> klass) {
		Type genericSuperclass = klass.getGenericSuperclass();
		if (genericSuperclass == null || !(genericSuperclass instanceof ParameterizedType))
			return new BoundType[0];

		return getStaticBoundTypes((ParameterizedType) genericSuperclass);
	}

	/**
	 * Returns the static bounds for the given field. <br>
	 * A static bound occurs if a subclass specifies the type parameter of the superclass explicitly.<br>
	 * Example: <code>class Foo { Collection&lt;Integer&gt; bar; }</code> would result in a
	 * <code>BoundType.of(Integer.class)</code>.
	 * 
	 * @param field
	 *        the field to examine
	 * @return all static bounds
	 */
	public static BoundType[] getStaticBoundTypes(Field field) {
		Type genericType = field.getGenericType();
		if (genericType == null || !(genericType instanceof ParameterizedType))
			return new BoundType[0];

		return getStaticBoundTypes((ParameterizedType) genericType);
	}

	/**
	 * Resolves the {@link TypeVariable} for a given {@link BoundType}.<br>
	 * Example: <code>class Foo&lt;T&gt; { Collection&lt;T&gt; bar; }; class IntFoo extends Foo&lt;Integer&gt; {}</code>
	 * .<br>
	 * The actual type of bar can be inferred for IntFoo and would result in a <code>BoundType.of(Integer.class)</code>.
	 * 
	 * @param type
	 *        the actual bound type
	 * @param typeVar
	 *        the placeholder type variable
	 * @return the bound type for the {@link TypeVariable}
	 */
	public static BoundType resolveType(BoundType type, TypeVariable<?> typeVar) {
		TypeVariable<?>[] typeParameters = type.getType().getTypeParameters();
		if (type.getParameters().length < typeParameters.length)
			return null;
		for (int index = 0; index < typeParameters.length; index++)
			if (typeVar.equals(typeParameters[index]))
				return type.getParameters()[index];
		return null;
	}

	/**
	 * Resolves the bindings of the direct {@link ParameterizedType} superclass for a given {@link BoundType}.<br>
	 * Example: <br>
	 * <code>class Pair&lt;S, T&gt; { }; <br>
	 * class OrderedPair&lt;X&gt; extends Pair&lt;X, X&gt; {}</code>.<br>
	 * If the actual type of X can be inferred for OrderedPair (hence BoundType), this method returns the actual bound
	 * for Pair.
	 * 
	 * @param type
	 *        the actual bound type
	 * @param superclass
	 *        the superclass of the wrapped type
	 * @return the bound type for the {@link ParameterizedType}
	 */
	public static BoundType getBindingsOfDirectSuperclass(BoundType type, ParameterizedType superclass) {
		Type[] actualTypeArguments = superclass.getActualTypeArguments();
		BoundType[] arguments = new BoundType[actualTypeArguments.length];
		for (int index = 0; index < actualTypeArguments.length; index++)
			if (actualTypeArguments[index] instanceof TypeVariable<?>)
				arguments[index] = resolveType(type, (TypeVariable<?>) actualTypeArguments[index]);
			else if (actualTypeArguments[index] instanceof ParameterizedType)
				arguments[index] = getBindingsOfDirectSuperclass(type, (ParameterizedType) actualTypeArguments[index]);
			else
				arguments[index] = BoundType.of((Class<?>) actualTypeArguments[index]);
		return BoundType.of((Class<?>) superclass.getRawType(), arguments);
	}

	/**
	 * Resolves the bindings of a superclass for a given {@link BoundType}.<br>
	 * Example: <br>
	 * <code>class Pair&lt;S, T&gt; { }; <br>
	 * class OrderedPair&lt;X&gt; extends Pair&lt;X, X&gt; {}
	 * class OrderedComparablePair&lt;X extends Comparable&lt;X&gt;&gt; extends OrderedPair&lt;X, X&gt; {}</code>.<br>
	 * If the actual type of X can be inferred for OrderedComparablePair (hence BoundType), this method returns the
	 * actual bound
	 * for Pair.
	 * 
	 * @param type
	 *        the actual bound type
	 * @param superClass
	 *        the superclass of the wrapped type
	 * @return the bound type for the {@link ParameterizedType}
	 */
	public static BoundType getBindingOfSuperclass(BoundType type, Class<?> superClass) {
		if (!superClass.isAssignableFrom(type.getType()))
			throw new IllegalArgumentException(type.getType() + " does not extend or implement " + superClass);
		if (superClass.getTypeParameters().length == 0)
			return BoundType.of(superClass);

		List<Type> hierarchy = getHierarchy(superClass, type.getType());
		return resolvePartially(type, hierarchy);
	}

	/**
	 * Resolves the bindings of a superclass for a given type.<br>
	 * Example: <br>
	 * <code>class Pair&lt;S, T&gt; { }; <br>
	 * class OrderedPair&lt;X&gt; extends Pair&lt;X, X&gt; {}<br> 
	 * class IntPair extends OrderedPair&lt;Integer, Integer&gt; {}<br> 
	 * getBindingOfSuperclass(IntPair.class, Pair.class) -&gt; BoundType.of(Pair.class, Integer.class, Integer.class)</code>
	 * 
	 * @param type
	 *        the actual bound type
	 * @param superClass
	 *        the superclass of the wrapped type
	 * @return the bound type for the {@link ParameterizedType}
	 */
	public static BoundType getBindingOfSuperclass(Class<?> type, Class<?> superClass) {
		if (!superClass.isAssignableFrom(type))
			throw new IllegalArgumentException(type + " does not extend or implement " + superClass);
		if (superClass.getTypeParameters().length == 0)
			return BoundType.of(superClass);

		List<Type> hierarchy = getHierarchy(superClass, type);
		return resolvePartially(BoundType.of(type), hierarchy);
	}

	private static BoundType resolvePartially(BoundType boundType, List<Type> hierarchy) {
		if (hierarchy.isEmpty())
			return boundType;
		Type type = hierarchy.get(0);
		if (type instanceof Class<?>)
			return resolvePartially(BoundType.of((Class<?>) type, boundType.getParameters()),
				hierarchy.subList(1, hierarchy.size()));
		return resolvePartially(getBindingsOfDirectSuperclass(boundType, (ParameterizedType) type),
			hierarchy.subList(1, hierarchy.size()));
	}

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
	public static Annotation getAnnotation(Class<?> type, Class<? extends Annotation> annotationType) {
		Annotation annotation = null;
		for (Class<?> t = type; annotation == null && t != null; t = t.getSuperclass())
			annotation = t.getAnnotation(annotationType);
		return annotation;
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
	public static Object invoke(Object object, String function, Object... params) {
		Class<? extends Object> type = object.getClass();
		try {
			Map<Method, Integer> candidateDistances = getCompatibleMethods(type, function, params);

			if (candidateDistances.isEmpty())
				throw new IllegalArgumentException(String.format(
					"no suitable method found in %s for name %s and parameters %s", type, function,
						Arrays.toString(params)));

			if (candidateDistances.size() == 1)
				return invoke(candidateDistances.keySet().iterator().next(), object, params);

			Method bestMethod = pickBest(candidateDistances);
			if (bestMethod == null)
				throw new IllegalArgumentException(String.format(
					"more than one suitable method found in %s for name %s and parameters %", type,
						function, Arrays.toString(params)));
			return invoke(bestMethod, object, params);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format(
				"Could not invoke method %s for type %s with parameters %s", function, type, Arrays
					.toString(params)), e);
		}
	}

	private static Object invoke(Method method, Object object, Object[] params) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		method.setAccessible(true);
		return method.invoke(object, params);
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
	public static boolean hasFunction(Object object, String function, Object... params) {
		Class<? extends Object> type = object.getClass();
		try {
			Map<Method, Integer> candidateDistances = getCompatibleMethods(type, function, params);

			if (candidateDistances.isEmpty())
				return false;

			if (candidateDistances.size() == 1)
				return true;

			return pickBest(candidateDistances) != null;
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Could not find method %s for type %s with parameters %s",
				function, type, Arrays
					.toString(params)), e);
		}
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
	public static Object getFieldValue(Object object, String fieldName) {
		Class<? extends Object> type = object.getClass();
		try {
			Field field = type.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Could not get field value %s for type %s", fieldName,
				type), e);
		}
	}
}
