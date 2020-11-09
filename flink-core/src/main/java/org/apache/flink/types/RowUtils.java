/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities to deal with {@link Row} instances.
 *
 * <p>This class exists to keep the {@link Row} class itself slim.
 */
@PublicEvolving
public final class RowUtils {

	// --------------------------------------------------------------------------------------------
	// Public utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Compares two {@link List}s of {@link Row} for deep equality. This method supports all conversion
	 * classes of the table ecosystem.
	 */
	public static boolean compareRows(List<Row> l1, List<Row> l2) {
		return compareRows(l1, l2, false);
	}

	/**
	 * Compares two {@link List}s of {@link Row} for deep equality. This method supports all conversion
	 * classes of the table ecosystem. The top-level lists can be compared with or without order.
	 */
	public static boolean compareRows(List<Row> l1, List<Row> l2, boolean ignoreOrder) {
		if (l1 == l2) {
			return true;
		} else if (l1 == null || l2 == null) {
			return false;
		}
		if (ignoreOrder) {
			return deepEqualsListUnordered(l1, l2);
		} else {
			return deepEqualsListOrdered(l1, l2);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Default scoped for Row class only
	// --------------------------------------------------------------------------------------------

	/**
	 * Compares two objects with proper (nested) equality semantics. This method supports all external
	 * and most internal conversion classes of the table ecosystem.
	 */
	static boolean deepEqualsRow(Row row1, Row row2) {
		if (row1.getKind() != row2.getKind()) {
			return false;
		}
		if (row1.getArity() != row2.getArity()) {
			return false;
		}
		for (int pos = 0; pos < row1.getArity(); pos++) {
			final Object f1 = row1.getField(pos);
			final Object f2 = row2.getField(pos);
			if (!deepEqualsInternal(f1, f2)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Hashes two objects with proper (nested) equality semantics. This method supports all external
	 * and most internal conversion classes of the table ecosystem.
	 */
	static int deepHashCodeRow(Row row) {
		int result = row.getKind().toByteValue(); // for stable hash across JVM instances
		for (int i = 0; i < row.getArity(); i++) {
			result = 31 * result + deepHashCodeInternal(row.getField(i));
		}
		return result;
	}

	// --------------------------------------------------------------------------------------------
	// Internal utilities
	// --------------------------------------------------------------------------------------------

	private static boolean deepEqualsInternal(Object o1, Object o2) {
		if (o1 == o2) {
			return true;
		} else if (o1 == null || o2 == null) {
			return false;
		} else if (o1 instanceof Row && o2 instanceof Row) {
			return deepEqualsRow((Row) o1, (Row) o2);
		} else if (o1 instanceof Object[] && o2 instanceof Object[]) {
			return deepEqualsArray((Object[]) o1, (Object[]) o2);
		} else if (o1 instanceof Map && o2 instanceof Map) {
			return deepEqualsMap((Map<?, ?>) o1, (Map<?, ?>) o2);
		} else if (o1 instanceof List && o2 instanceof List) {
			return deepEqualsListOrdered((List<?>) o1, (List<?>) o2);
		}
		return Objects.deepEquals(o1, o2);
	}

	private static boolean deepEqualsArray(Object[] a1, Object[] a2) {
		if (a1.getClass() != a2.getClass()) {
			return false;
		}
		if (a1.length != a2.length) {
			return false;
		}
		for (int pos = 0; pos < a1.length; pos++) {
			final Object e1 = a1[pos];
			final Object e2 = a2[pos];
			if (!deepEqualsInternal(e1, e2)) {
				return false;
			}
		}
		return true;
	}

	private static <K, V> boolean deepEqualsMap(Map<K, V> m1, Map<?, ?> m2) {
		// copied from HashMap.equals but with deepEquals comparision
		if (m1.size() != m2.size()) {
			return false;
		}
		try {
			for (Map.Entry<K, V> e : m1.entrySet()) {
				K key = e.getKey();
				V value = e.getValue();
				if (value == null) {
					if (!(m2.get(key) == null && m2.containsKey(key))) {
						return false;
					}
				} else {
					if (!deepEqualsInternal(value, m2.get(key))) {
						return false;
					}
				}
			}
		} catch (ClassCastException | NullPointerException unused) {
			return false;
		}
		return true;
	}

	private static <E> boolean deepEqualsListOrdered(List<E> l1, List<?> l2) {
		if (l1.size() != l2.size()) {
			return false;
		}
		final Iterator<E> i1 = l1.iterator();
		final Iterator<?> i2 = l2.iterator();
		while (i1.hasNext() && i2.hasNext()) {
			final E o1 = i1.next();
			final Object o2 = i2.next();
			if (!deepEqualsInternal(o1, o2)) {
				return false;
			}
		}
		return true;
	}

	private static <E> boolean deepEqualsListUnordered(List<E> l1, List<?> l2) {
		final List<?> l2Mutable = new LinkedList<>(l2);
		for (E e1 : l1) {
			final Iterator<?> iterator = l2Mutable.iterator();
			boolean found = false;
			while (iterator.hasNext()) {
				final Object e2 = iterator.next();
				if (deepEqualsInternal(e1, e2)) {
					found = true;
					iterator.remove();
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		return l2Mutable.size() == 0;
	}

	private static int deepHashCodeInternal(Object o) {
		if (o == null) {
			return 0;
		}
		if (o instanceof Row) {
			return deepHashCodeRow((Row) o);
		} else if (o instanceof Object[]) {
			return deepHashCodeArray((Object[]) o);
		} else if (o instanceof Map) {
			return deepHashCodeMap((Map<?, ?>) o);
		} else if (o instanceof List) {
			return deepHashCodeList((List<?>) o);
		}
		return Arrays.deepHashCode(new Object[]{o});
	}

	private static int deepHashCodeArray(Object[] a) {
		int result = 1;
		for (Object element : a) {
			result = 31 * result + deepHashCodeInternal(element);
		}
		return result;
	}

	private static int deepHashCodeMap(Map<?, ?> m) {
		int result = 1;
		for (Map.Entry<?, ?> entry : m.entrySet()) {
			result += deepHashCodeInternal(entry.getKey()) ^ deepHashCodeInternal(entry.getValue());
		}
		return result;
	}

	private static int deepHashCodeList(List<?> l) {
		int result = 1;
		for (Object e : l) {
			result = 31 * result + deepHashCodeInternal(e);
		}
        return result;
	}

	private RowUtils() {
		// no instantiation
	}
}
