package eu.stratosphere.util.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convenience methods for retrieving bound types of generic classes through hierarchical binding.
 * 
 * @author Arvid Heise
 */
public class BoundTypeUtil {
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
	public static BoundType getBindingOfSuperclass(final BoundType type, final Class<?> superClass) {
		if (!superClass.isAssignableFrom(type.getType()))
			throw new IllegalArgumentException(type.getType() + " does not extend or implement " + superClass);
		if (superClass.getTypeParameters().length == 0)
			return BoundType.of(superClass);

		final List<Type> hierarchy = getHierarchy(superClass, type.getType());
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
	public static BoundType getBindingOfSuperclass(final Class<?> type, final Class<?> superClass) {
		if (!superClass.isAssignableFrom(type))
			throw new IllegalArgumentException(type + " does not extend or implement " + superClass);
		if (superClass.getTypeParameters().length == 0)
			return BoundType.of(superClass);

		final List<Type> hierarchy = getHierarchy(superClass, type);
		return resolvePartially(BoundType.of(type), hierarchy);
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
	public static BoundType getBindingsOfDirectSuperclass(final BoundType type, final ParameterizedType superclass) {
		final Type[] actualTypeArguments = superclass.getActualTypeArguments();
		final BoundType[] arguments = new BoundType[actualTypeArguments.length];
		for (int index = 0; index < actualTypeArguments.length; index++)
			if (actualTypeArguments[index] instanceof TypeVariable<?>)
				arguments[index] = resolveType(type, (TypeVariable<?>) actualTypeArguments[index]);
			else if (actualTypeArguments[index] instanceof ParameterizedType)
				arguments[index] = getBindingsOfDirectSuperclass(type, (ParameterizedType) actualTypeArguments[index]);
			else
				arguments[index] = BoundType.of((Class<?>) actualTypeArguments[index]);
		return BoundType.of((Class<?>) superclass.getRawType(), arguments);
	}

	private static List<Type> getHierarchy(final Class<?> superClass, final Class<?> subclass) {
		if (!superClass.isAssignableFrom(subclass))
			throw new IllegalArgumentException();

		List<Type> hierarchy = new ArrayList<Type>();
		if (superClass == subclass)
			return hierarchy;

		if (superClass.isInterface()) {
			final Type[] interfaces = subclass.getGenericInterfaces();
			int minDistance = Integer.MAX_VALUE;
			for (final Type xface : interfaces) {

				final Class<?> type = (Class<?>) (xface instanceof Class ? xface : ((ParameterizedType) xface)
					.getRawType());
				if (type == superClass) {
					hierarchy.clear();
					hierarchy.add(xface);
					break;
				}

				if (superClass.isAssignableFrom(type)) {
					final List<Type> partialHierarchy = getHierarchy(superClass, type);
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
		Class<?> rawType = subclass;
		do {
			hierarchy.add(clazz);
			rawType = (Class<?>) (clazz instanceof Class ? clazz : ((ParameterizedType) clazz).getRawType());
			clazz = rawType.getGenericSuperclass();
		} while (superClass != rawType);
		return hierarchy;
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
	public static BoundType[] getStaticBoundTypes(final Class<?> klass) {
		final Type genericSuperclass = klass.getGenericSuperclass();
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
	public static BoundType[] getStaticBoundTypes(final Field field) {
		final Type genericType = field.getGenericType();
		if (genericType == null || !(genericType instanceof ParameterizedType))
			return new BoundType[0];

		return getStaticBoundTypes((ParameterizedType) genericType);
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
	public static BoundType[] getStaticBoundTypes(final ParameterizedType parameterizedType) {
		final List<BoundType> boundedTypes = new ArrayList<BoundType>();
		for (final Type type : parameterizedType.getActualTypeArguments())
			if (type instanceof Class<?>)
				boundedTypes.add(new BoundType((Class<?>) type));
			else if (type instanceof ParameterizedType)
				boundedTypes.add(new BoundType((ParameterizedType) type));

		final Class<?> rawType = (Class<?>) parameterizedType.getRawType();
		if (rawType.getGenericSuperclass() instanceof ParameterizedType)
			boundedTypes.addAll(0,
				Arrays.asList(getStaticBoundTypes((ParameterizedType) rawType.getGenericSuperclass())));

		return boundedTypes.toArray(new BoundType[boundedTypes.size()]);
	}

	private static BoundType resolvePartially(final BoundType boundType, final List<Type> hierarchy) {
		if (hierarchy.isEmpty())
			return boundType;
		final Type type = hierarchy.get(0);
		if (type instanceof Class<?>)
			return resolvePartially(BoundType.of((Class<?>) type, boundType.getParameters()),
				hierarchy.subList(1, hierarchy.size()));
		return resolvePartially(getBindingsOfDirectSuperclass(boundType, (ParameterizedType) type),
			hierarchy.subList(1, hierarchy.size()));
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
	public static BoundType resolveType(final BoundType type, final TypeVariable<?> typeVar) {
		final TypeVariable<?>[] typeParameters = type.getType().getTypeParameters();
		if (type.getParameters().length < typeParameters.length)
			return null;
		for (int index = 0; index < typeParameters.length; index++)
			if (typeVar.equals(typeParameters[index]))
				return type.getParameters()[index];
		return null;
	}
}
