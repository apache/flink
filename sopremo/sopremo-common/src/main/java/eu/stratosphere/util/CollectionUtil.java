package eu.stratosphere.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CollectionUtil {
	public static void ensureSize(final Collection<?> collection, final int size) {
		while (collection.size() < size)
			collection.add(null);
	}

	public static <T> Iterable<T> mergeUnique(final Iterable<T>... iterables) {
		switch (iterables.length) {
		case 0:
			return Collections.emptyList();
		case 1:
			return iterables[0];
		default:
			return new WrappingIterable<T, T>(new ConcatenatingIterable<T>(iterables)) {
				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.util.WrappingIterable#wrap(java.util.Iterator)
				 */
				@Override
				protected Iterator<T> wrap(Iterator<T> iterator) {
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
	}
}
