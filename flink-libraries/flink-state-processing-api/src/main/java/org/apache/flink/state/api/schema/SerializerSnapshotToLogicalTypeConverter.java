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

package org.apache.flink.state.api.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer.NullableSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerSnapshot;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@link TypeSerializerSnapshot} tree into a Flink SQL {@link LogicalType}.
 *
 * <p>This is a pure function — no I/O, no class loading beyond what the snapshot itself already
 * contains. Field names are extracted from {@link PojoSerializerSnapshot} entries; all other
 * composite types use positional names {@code f0, f1, …}.
 *
 * <p>flink-avro is an optional dependency of this module (see the module {@code pom.xml}), so this
 * class must never mention an Avro type directly: doing so - even in an {@code instanceof} branch -
 * would make the JVM try to resolve that type the moment this method is reached for *any*
 * unrecognized snapshot (e.g. a plain {@code ListSerializerSnapshot}), throwing {@code
 * NoClassDefFoundError} for callers who never use Avro at all. The Avro-specific branch is instead
 * selected by class name and delegated to {@link AvroStateUtils}, which is only ever loaded once
 * that name comparison has already confirmed Avro is genuinely on the classpath.
 */
@Internal
public final class SerializerSnapshotToLogicalTypeConverter {

    /** Type used for snapshots we cannot describe any further, e.g. a missing nested snapshot. */
    private static final LogicalType OPAQUE_TYPE =
            new VarBinaryType(true, VarBinaryType.MAX_LENGTH);

    private static final TypeSerializerSnapshot<?>[] NO_NESTED_SNAPSHOTS =
            new TypeSerializerSnapshot<?>[0];

    /**
     * Snapshot classes that always map to the same {@link LogicalType}. Matched by exact class
     * because every one of them is final; {@link LogicalType} instances are immutable and therefore
     * safe to share.
     *
     * <p>The window entries are not namespace-specific: they fire identically if a window type is
     * ever used as an ordinary value rather than as a namespace.
     */
    private static final Map<Class<?>, LogicalType> FIXED_TYPES = createFixedTypes();

    private SerializerSnapshotToLogicalTypeConverter() {}

    private static Map<Class<?>, LogicalType> createFixedTypes() {
        Map<Class<?>, LogicalType> types = new HashMap<>();
        types.put(IntSerializer.IntSerializerSnapshot.class, new IntType(false));
        types.put(LongSerializer.LongSerializerSnapshot.class, new BigIntType(false));
        types.put(FloatSerializer.FloatSerializerSnapshot.class, new FloatType(false));
        types.put(DoubleSerializer.DoubleSerializerSnapshot.class, new DoubleType(false));
        types.put(BooleanSerializer.BooleanSerializerSnapshot.class, new BooleanType(false));
        types.put(ByteSerializer.ByteSerializerSnapshot.class, new TinyIntType(false));
        types.put(ShortSerializer.ShortSerializerSnapshot.class, new SmallIntType(false));
        types.put(CharSerializer.CharSerializerSnapshot.class, new CharType(false, 1));
        types.put(
                StringSerializer.StringSerializerSnapshot.class,
                new VarCharType(true, VarCharType.MAX_LENGTH));
        types.put(
                TimeWindow.Serializer.TimeWindowSerializerSnapshot.class,
                new RowType(
                        false,
                        List.of(
                                new RowType.RowField("window_start", new TimestampType(false, 3)),
                                new RowType.RowField("window_end", new TimestampType(false, 3)))));
        types.put(
                GlobalWindow.Serializer.GlobalWindowSerializerSnapshot.class,
                new RowType(false, List.of()));
        return types;
    }

    /**
     * Converts the given snapshot to a {@link LogicalType}.
     *
     * @param snapshot the serializer snapshot to convert, may be null
     * @return a {@link LogicalType} corresponding to the snapshot type
     * @throws UnsupportedOperationException if the snapshot type is not supported for schema-based
     *     table access
     */
    public static LogicalType convert(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot == null) {
            return OPAQUE_TYPE;
        }

        LogicalType fixedType = FIXED_TYPES.get(snapshot.getClass());
        if (fixedType != null) {
            return fixedType;
        }

        if (snapshot instanceof PojoSerializerSnapshot) {
            return convertPojo((PojoSerializerSnapshot<?>) snapshot);
        }
        if (snapshot instanceof EnumSerializer.EnumSerializerSnapshot) {
            return new VarCharType(true, VarCharType.MAX_LENGTH);
        }
        String avroSnapshotClassName = AvroStateUtils.AVRO_SERIALIZER_SNAPSHOT_CLASS_NAME;
        if (avroSnapshotClassName.equals(snapshot.getClass().getName())) {
            return AvroStateUtils.convertToLogicalType(snapshot);
        }
        if (snapshot instanceof ListSerializerSnapshot) {
            return new ArrayType(true, convertNested(snapshot, 0));
        }
        if (snapshot instanceof MapSerializerSnapshot) {
            return new MapType(true, convertNested(snapshot, 0), convertNested(snapshot, 1));
        }
        if (snapshot instanceof NullableSerializerSnapshot) {
            return convertNested(snapshot, 0).copy(true);
        }
        if (snapshot instanceof TupleSerializerSnapshot) {
            return convertTuple((TupleSerializerSnapshot<?>) snapshot);
        }
        if (snapshot instanceof RowDataSerializerSnapshot) {
            return convertRowData((RowDataSerializerSnapshot) snapshot);
        }

        throw new UnsupportedOperationException(
                "Cannot extract schema for TypeSerializerSnapshot of type '"
                        + snapshot.getClass().getName()
                        + "'. This serializer type is not supported for schema-based table"
                        + " access.");
    }

    private static LogicalType convertPojo(PojoSerializerSnapshot<?> snapshot) {
        List<AbstractMap.SimpleEntry<String, TypeSerializerSnapshot<?>>> fieldEntries =
                snapshot.getFieldSnapshotEntries();
        List<RowType.RowField> fields = new ArrayList<>(fieldEntries.size());
        for (AbstractMap.SimpleEntry<String, TypeSerializerSnapshot<?>> entry : fieldEntries) {
            fields.add(new RowType.RowField(entry.getKey(), convert(entry.getValue())));
        }
        return new RowType(true, fields);
    }

    private static LogicalType convertTuple(TupleSerializerSnapshot<?> snapshot) {
        TypeSerializerSnapshot<?>[] nested = nestedSnapshots(snapshot);
        List<RowType.RowField> fields = new ArrayList<>(nested.length);
        for (int i = 0; i < nested.length; i++) {
            fields.add(new RowType.RowField("f" + i, convert(nested[i])));
        }
        return new RowType(true, fields);
    }

    /**
     * Converts a {@link RowDataSerializerSnapshot} from its {@link
     * RowDataSerializerSnapshot#getTypes()} instead of walking its nested field snapshots: a {@link
     * LogicalType} is already a complete, self-describing schema (including nested field names),
     * unlike the POJO/Avro case where the snapshot tree is the only source of field names.
     */
    private static LogicalType convertRowData(RowDataSerializerSnapshot snapshot) {
        LogicalType[] types = snapshot.getTypes();
        String[] fieldNames = snapshot.getFieldNames();
        List<RowType.RowField> fields = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++) {
            String fieldName = fieldNames != null ? fieldNames[i] : "f" + i;
            fields.add(new RowType.RowField(fieldName, types[i]));
        }
        return new RowType(true, fields);
    }

    private static LogicalType convertNested(TypeSerializerSnapshot<?> snapshot, int index) {
        TypeSerializerSnapshot<?>[] nested = nestedSnapshots(snapshot);
        return convert(index < nested.length ? nested[index] : null);
    }

    private static TypeSerializerSnapshot<?>[] nestedSnapshots(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot instanceof CompositeTypeSerializerSnapshot) {
            TypeSerializerSnapshot<?>[] nested =
                    ((CompositeTypeSerializerSnapshot<?, ?>) snapshot)
                            .getNestedSerializerSnapshots();
            if (nested != null) {
                return nested;
            }
        }
        return NO_NESTED_SNAPSHOTS;
    }
}
