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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
     * Compares two {@link List}s of {@link Row} for deep equality. This method supports all
     * conversion classes of the table ecosystem.
     */
    public static boolean compareRows(List<Row> l1, List<Row> l2) {
        return compareRows(l1, l2, false);
    }

    /**
     * Compares two {@link List}s of {@link Row} for deep equality. This method supports all
     * conversion classes of the table ecosystem. The top-level lists can be compared with or
     * without order.
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
    // Internal utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Internal flag to enable the legacy {@link Row#toString()} implementation for tests. In
     * general, tests should not depend on the string representation of rows but should fully
     * compare instances (especially data types of fields). This flag will be dropped once all tests
     * have been updated.
     */
    public static boolean USE_LEGACY_TO_STRING = false;

    /** Internal utility for creating a row in static named-position field mode. */
    @Internal
    public static Row createRowWithNamedPositions(
            RowKind kind, Object[] fieldByPosition, LinkedHashMap<String, Integer> positionByName) {
        return new Row(kind, fieldByPosition, null, positionByName);
    }

    // --------------------------------------------------------------------------------------------
    // Default scoped for Row class only
    // --------------------------------------------------------------------------------------------

    /**
     * Compares two objects with proper (nested) equality semantics. This method supports all
     * external and most internal conversion classes of the table ecosystem.
     */
    static boolean deepEqualsRow(
            RowKind kind1,
            @Nullable Object[] fieldByPosition1,
            @Nullable Map<String, Object> fieldByName1,
            @Nullable LinkedHashMap<String, Integer> positionByName1,
            RowKind kind2,
            @Nullable Object[] fieldByPosition2,
            @Nullable Map<String, Object> fieldByName2,
            @Nullable LinkedHashMap<String, Integer> positionByName2) {
        if (kind1 != kind2) {
            return false;
        }
        // positioned == positioned
        else if (fieldByPosition1 != null && fieldByPosition2 != null) {
            // positionByName is not included
            return deepEqualsInternal(fieldByPosition1, fieldByPosition2);
        }
        // named == named
        else if (fieldByName1 != null && fieldByName2 != null) {
            return deepEqualsInternal(fieldByName1, fieldByName2);
        }
        // named positioned == named
        else if (positionByName1 != null && fieldByName2 != null) {
            return deepEqualsNamedRows(fieldByPosition1, positionByName1, fieldByName2);
        }
        // named == named positioned
        else if (positionByName2 != null && fieldByName1 != null) {
            return deepEqualsNamedRows(fieldByPosition2, positionByName2, fieldByName1);
        }
        return false;
    }

    /**
     * Hashes two objects with proper (nested) equality semantics. This method supports all external
     * and most internal conversion classes of the table ecosystem.
     */
    static int deepHashCodeRow(
            RowKind kind,
            @Nullable Object[] fieldByPosition,
            @Nullable Map<String, Object> fieldByName) {
        int result = kind.toByteValue(); // for stable hash across JVM instances
        if (fieldByPosition != null) {
            // positionByName is not included
            result = 31 * result + deepHashCodeInternal(fieldByPosition);
        } else {
            result = 31 * result + deepHashCodeInternal(fieldByName);
        }
        return result;
    }

    /**
     * Converts a row to a string representation. This method supports all external and most
     * internal conversion classes of the table ecosystem.
     */
    static String deepToStringRow(
            RowKind kind,
            @Nullable Object[] fieldByPosition,
            @Nullable Map<String, Object> fieldByName) {
        final StringBuilder sb = new StringBuilder();
        if (fieldByPosition != null) {
            if (USE_LEGACY_TO_STRING) {
                deepToStringArrayLegacy(sb, fieldByPosition);
            } else {
                sb.append(kind.shortString());
                deepToStringArray(sb, fieldByPosition);
            }
        } else {
            assert fieldByName != null;
            sb.append(kind.shortString());
            deepToStringMap(sb, fieldByName);
        }
        return sb.toString();
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static boolean deepEqualsNamedRows(
            Object[] fieldByPosition1,
            LinkedHashMap<String, Integer> positionByName1,
            Map<String, Object> fieldByName2) {
        for (Map.Entry<String, Object> entry : fieldByName2.entrySet()) {
            final Integer pos = positionByName1.get(entry.getKey());
            if (pos == null) {
                return false;
            }
            if (!deepEqualsInternal(fieldByPosition1[pos], entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean deepEqualsInternal(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        } else if (o1 == null || o2 == null) {
            return false;
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
        // copied from HashMap.equals but with deepEquals comparison
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
        if (o instanceof Object[]) {
            return deepHashCodeArray((Object[]) o);
        } else if (o instanceof Map) {
            return deepHashCodeMap((Map<?, ?>) o);
        } else if (o instanceof List) {
            return deepHashCodeList((List<?>) o);
        }
        return Arrays.deepHashCode(new Object[] {o});
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

    private static void deepToStringInternal(StringBuilder sb, Object o) {
        if (o instanceof Object[]) {
            deepToStringArray(sb, (Object[]) o);
        } else if (o instanceof Map) {
            deepToStringMap(sb, (Map<?, ?>) o);
        } else if (o instanceof List) {
            deepToStringList(sb, (List<?>) o);
        } else {
            sb.append(StringUtils.arrayAwareToString(o));
        }
    }

    private static void deepToStringArray(StringBuilder sb, Object[] a) {
        sb.append('[');
        boolean isFirst = true;
        for (Object o : a) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            deepToStringInternal(sb, o);
        }
        sb.append(']');
    }

    private static void deepToStringArrayLegacy(StringBuilder sb, Object[] a) {
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(StringUtils.arrayAwareToString(a[i]));
        }
    }

    private static <K, V> void deepToStringMap(StringBuilder sb, Map<K, V> m) {
        sb.append('{');
        boolean isFirst = true;
        for (Map.Entry<K, V> entry : m.entrySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            deepToStringInternal(sb, entry.getKey());
            sb.append('=');
            deepToStringInternal(sb, entry.getValue());
        }
        sb.append('}');
    }

    private static <E> void deepToStringList(StringBuilder sb, List<E> l) {
        sb.append('[');
        boolean isFirst = true;
        for (E element : l) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            deepToStringInternal(sb, element);
        }
        sb.append(']');
    }

    private RowUtils() {
        // no instantiation
    }
}
