package eu.stratosphere.util;

import java.util.Collection;

public class CollectionUtil {
	public static void ensureSize(Collection<?> collection, int size) {
		while(collection.size() < size)
			collection.add(null);
	}
}
