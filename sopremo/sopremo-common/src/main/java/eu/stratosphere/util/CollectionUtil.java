package eu.stratosphere.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CollectionUtil {
	/**
	 * Pads the given collection with <code>null</code>s until the collection has at least the given size.
	 * 
	 * @param collection
	 *        the collection to pad
	 * @param size
	 *        the desired minimum size
	 */
	public static void ensureSize(final Collection<?> collection, final int size) {
		ensureSize(collection, size, null);
	}

	/**
	 * Concatenates the given {@link Iterable}s and removes duplicate entries on the fly.<br>
	 * The result is not materialized and thus changes to the parameters is also reflected in the result.
	 * 
	 * @param iterables
	 *        the iterables to merge
	 * @return a duplicate-free {@link Iterable}
	 */
	public static <T> Iterable<T> mergeUnique(final Iterable<? extends Iterable<? extends T>> iterables) {
		return new WrappingIterable<T, T>(new ConcatenatingIterable<T>(iterables)) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.WrappingIterable#wrap(java.util.Iterator)
			 */
			@Override
			protected Iterator<T> wrap(Iterator<? extends T> iterator) {
				final Set<T> alreadySeen = new HashSet<T>();
				return new FilteringIterator<T>(iterator, new Predicate<T>() {
					@Override
					public boolean isTrue(T param) {
						return alreadySeen.add(param);
					};
				});
			}
		};
	}

	/**
	 * Pads the given collection with the given default value until the collection has at least the given size.
	 * 
	 * @param collection
	 *        the collection to pad
	 * @param size
	 *        the desired minimum size
	 * @param defaultValue
	 *        the default value
	 */
	public static <T> void ensureSize(Collection<T> collection, int size, T defaultValue) {
		while (collection.size() < size)
			collection.add(defaultValue);
	}
}
