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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.input.deserializer.InternalTypeConverter;
import org.apache.flink.state.api.schema.AvroStateUtils;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Converts Java objects returned by Flink state (POJOs, Avro records, primitives) into their
 * internal {@link RowData} representation, driven by a target {@link LogicalType}. Shared by all
 * state reader functions and non-keyed row mappers.
 *
 * <p>flink-avro is an optional dependency of this module (see the module {@code pom.xml}), so this
 * class must never mention an Avro type directly: doing so - even in an {@code instanceof} branch -
 * would make the JVM try to resolve that type the moment this method is reached for *any*
 * row-shaped value, throwing {@code NoClassDefFoundError} for callers who never use Avro at all.
 * Avro values are instead recognized and read via {@link AvroStateUtils}, which is only ever loaded
 * once it has already confirmed, by interface name, that the value is genuinely Avro-backed.
 */
@Internal
@SuppressWarnings({"rawtypes", "unchecked"})
class StateValueConverter implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    // Field/Method are not serializable; the reader function containing this converter is shipped
    // to task managers via Java serialization, so these are rebuilt lazily on first use. The
    // Optional value distinguishes a cached "not found" from "not yet looked up", so a missing
    // field/getter is only ever resolved via reflection once per class instead of on every row.
    private transient Map<Tuple2<Class, String>, Optional<Field>> classFieldCache;
    private transient Map<Tuple2<Class, String>, Optional<Method>> classMethodCache;

    private Map<Tuple2<Class, String>, Optional<Field>> classFieldCache() {
        if (classFieldCache == null) {
            classFieldCache = new ConcurrentHashMap<>();
        }
        return classFieldCache;
    }

    private Map<Tuple2<Class, String>, Optional<Method>> classMethodCache() {
        if (classMethodCache == null) {
            classMethodCache = new ConcurrentHashMap<>();
        }
        return classMethodCache;
    }

    /**
     * Reads the value from a VALUE-shaped state, dispatching to {@code ValueState.value()}, {@code
     * ReducingState.get()} or {@code AggregatingState.get()} depending on {@code actualStateKind}
     * (see {@link AbstractSavepointDataStreamScanProvider#buildStateDescriptor}).
     */
    static Object readValueLikeState(State state, StateDescriptor.Type actualStateKind)
            throws Exception {
        switch (actualStateKind) {
            case REDUCING:
                return ((ReducingState) state).get();
            case AGGREGATING:
                return ((AggregatingState) state).get();
            default:
                return ((ValueState) state).value();
        }
    }

    /**
     * Reads a LIST state's elements for the {@code (state_key, ..., list_value)} (non-flattened)
     * table, normalizing the state backend's {@code null} — returned by both {@code HeapListState}
     * and {@code RocksDBListState} when the current key/namespace has no entries — to an empty
     * {@link List}. This mirrors {@code MapState.entries()}, which already returns an empty (never
     * {@code null}) {@link Iterable} in that same case, so LIST- and MAP-shaped state consistently
     * always have a value (possibly empty) as documented on {@code StateTableUtils}, rather than
     * the LIST column surfacing SQL {@code NULL} for a key that other states in the same row do
     * have data for.
     */
    static Iterable<Object> readListLikeState(ListState<Object> state) throws Exception {
        Iterable<Object> values = state.get();
        return values == null ? Collections.emptyList() : values;
    }

    /**
     * Iterates a flattened LIST state's elements, emitting one row per element via {@code out}.
     * Each row is created via {@code rowTemplate} (which supplies the leading columns already
     * populated — e.g. the key, and for namespaced states, the window), and this method fills in
     * the list index and value at {@code subKeyColumnIndex}/{@code valueColumnIndex}.
     */
    void writeListRows(
            Iterable<Object> values,
            Supplier<GenericRowData> rowTemplate,
            int subKeyColumnIndex,
            int valueColumnIndex,
            LogicalType indexLogicalType,
            LogicalType valueLogicalType,
            Collector<RowData> out) {
        if (values == null) {
            return;
        }
        long index = 0;
        for (Object value : values) {
            GenericRowData row = rowTemplate.get();
            row.setField(subKeyColumnIndex, getValue(indexLogicalType, index));
            row.setField(valueColumnIndex, getValue(valueLogicalType, value));
            out.collect(row);
            index++;
        }
    }

    /**
     * Iterates a flattened MAP state's entries, emitting one row per entry via {@code out}. Mirrors
     * {@link #writeListRows} for MAP-shaped state.
     */
    void writeMapRows(
            Iterable<Map.Entry<Object, Object>> entries,
            Supplier<GenericRowData> rowTemplate,
            int mapKeyColumnIndex,
            int valueColumnIndex,
            LogicalType mapKeyLogicalType,
            LogicalType valueLogicalType,
            Collector<RowData> out) {
        if (entries == null) {
            return;
        }
        for (Map.Entry<Object, Object> entry : entries) {
            GenericRowData row = rowTemplate.get();
            row.setField(mapKeyColumnIndex, getValue(mapKeyLogicalType, entry.getKey()));
            row.setField(valueColumnIndex, getValue(valueLogicalType, entry.getValue()));
            out.collect(row);
        }
    }

    Object getValue(LogicalType logicalType, Object object) {
        if (object == null) {
            return null;
        }
        switch (logicalType.getTypeRoot()) {
            case ROW:
                if (object instanceof TimeWindow) {
                    TimeWindow window = (TimeWindow) object;
                    GenericRowData result = new GenericRowData(RowKind.INSERT, 2);
                    result.setField(0, TimestampData.fromEpochMillis(window.getStart()));
                    result.setField(1, TimestampData.fromEpochMillis(window.getEnd()));
                    return result;
                }
                if (object instanceof GenericRowData) {
                    // Copy by position into a fresh INSERT row rather than returning the source
                    // as-is: a coincidentally equal arity does not imply matching field ordering
                    // or semantics (e.g. after schema evolution or a column reorder).
                    GenericRowData sourceRow = (GenericRowData) object;
                    int targetFieldCount = ((RowType) logicalType).getFieldCount();
                    GenericRowData result = new GenericRowData(RowKind.INSERT, targetFieldCount);
                    for (int i = 0; i < targetFieldCount; i++) {
                        result.setField(i, i < sourceRow.getArity() ? sourceRow.getField(i) : null);
                    }
                    return result;
                }
                return convertToRow(object, logicalType);
            default:
                return InternalTypeConverter.toInternal(object, logicalType);
        }
    }

    private GenericRowData convertToRow(Object object, LogicalType logicalType) {
        RowType rowType = (RowType) logicalType;
        GenericRowData result = new GenericRowData(RowKind.INSERT, rowType.getFieldCount());
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField subRowField = fields.get(i);
            result.setField(
                    i, getValue(subRowField.getType(), getObjectField(object, subRowField)));
        }
        return result;
    }

    private Object getObjectField(Object object, RowType.RowField rowField) {
        String rowFieldName = rowField.getName();
        Class objectClass = object.getClass();

        // Avro GenericRecord: use the typed API directly. AvroStateUtils.isGenericRecord() caches
        // its own answer per class, so this is cheap on every call after the first for a class.
        if (AvroStateUtils.isGenericRecord(objectClass)) {
            return AvroStateUtils.getGenericRecordField(object, rowFieldName);
        }

        Optional<Field> field =
                classFieldCache()
                        .computeIfAbsent(
                                Tuple2.of(objectClass, rowFieldName),
                                key -> lookupField(objectClass, rowFieldName));
        if (field.isPresent()) {
            try {
                return field.get().get(object);
            } catch (IllegalAccessException e) {
                throw new UnsupportedOperationException(
                        "Cannot access field by either public member or getter function: "
                                + rowFieldName);
            }
        }

        Method getter = getGetter(objectClass, rowFieldName);
        if (getter == null) {
            throw new UnsupportedOperationException(
                    "Cannot access field by either public member or getter function: "
                            + rowFieldName);
        }
        try {
            return getter.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<Field> lookupField(Class objectClass, String fieldName) {
        try {
            return Optional.of(objectClass.getField(fieldName));
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        }
    }

    private Method getGetter(Class objectClass, String rowFieldName) {
        String capitalized = rowFieldName.substring(0, 1).toUpperCase() + rowFieldName.substring(1);
        Optional<Method> getter = lookupMethod(objectClass, "get" + capitalized);
        if (getter.isPresent()) {
            return getter.get();
        }
        return lookupMethod(objectClass, "is" + capitalized).orElse(null);
    }

    private Optional<Method> lookupMethod(Class objectClass, String methodName) {
        return classMethodCache()
                .computeIfAbsent(
                        Tuple2.of(objectClass, methodName),
                        key -> {
                            try {
                                return Optional.of(objectClass.getMethod(methodName));
                            } catch (NoSuchMethodException e) {
                                return Optional.empty();
                            }
                        });
    }
}
