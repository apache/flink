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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializerSnapshot;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * All Avro-specific logic used by the State Processing API's schema-based table access, gathered in
 * one place so that the classes which dispatch to it never need to mention an Avro type themselves.
 *
 * <p>flink-avro is an optional dependency of this module (see the module {@code pom.xml}). A class
 * that mentions an Avro type directly - even in an {@code instanceof} branch that is never taken -
 * makes the JVM try to resolve that type the moment the check is reached for *any* value, throwing
 * {@code NoClassDefFoundError} for callers who never use Avro at all. Callers must therefore select
 * the Avro-specific branch by class/interface name (see {@link
 * #AVRO_SERIALIZER_SNAPSHOT_CLASS_NAME} and {@link #isGenericRecord}) and only then delegate here;
 * this class is consequently only ever loaded once that name comparison has already confirmed Avro
 * is genuinely on the classpath.
 */
@Internal
public final class AvroStateUtils {

    /** Fully-qualified name of {@code AvroSerializerSnapshot}, for dispatch by class name. */
    public static final String AVRO_SERIALIZER_SNAPSHOT_CLASS_NAME =
            "org.apache.flink.formats.avro.typeutils.AvroSerializerSnapshot";

    private static final String GENERIC_RECORD_CLASS_NAME = "org.apache.avro.generic.GenericRecord";

    /**
     * Memoizes {@link #isGenericRecord}: it walks a class's full interface hierarchy, so callers
     * that check the same class repeatedly (e.g. once per field of every row of the same type)
     * would otherwise repeat that walk every time. A plain static map is safe here since the answer
     * is a pure function of the {@code Class} object - it never changes for a given class, and is
     * shared happily across every {@link AvroStateUtils} caller and instance.
     */
    private static final Map<Class<?>, Boolean> GENERIC_RECORD_CACHE = new ConcurrentHashMap<>();

    private AvroStateUtils() {}

    /**
     * Builds the fallback serializer for an {@code AvroSerializerSnapshot} with a missing class.
     */
    public static TypeSerializer<?> createFallbackSerializer(TypeSerializerSnapshot<?> snapshot) {
        Schema schema = ((AvroSerializerSnapshot<?>) snapshot).getSchema();
        return new AvroSerializer<>(GenericRecord.class, schema);
    }

    /**
     * Converts an {@code AvroSerializerSnapshot}'s embedded writer schema into a {@link
     * LogicalType}.
     */
    public static LogicalType convertToLogicalType(TypeSerializerSnapshot<?> snapshot) {
        // getSchema() returns the writer schema embedded in the snapshot — always present
        // regardless of whether the specific record class is on the classpath.
        AvroSerializerSnapshot<?> avroSnapshot = (AvroSerializerSnapshot<?>) snapshot;
        return AvroSchemaConverter.convertToDataType(avroSnapshot.getSchema().toString())
                .getLogicalType();
    }

    /**
     * Returns {@code true} if {@code clazz}, or any class/interface in its hierarchy, is named
     * {@code org.apache.avro.generic.GenericRecord}. Safe to call even when {@code GenericRecord}
     * itself is not on the classpath: {@code clazz} could only have been loaded and instantiated if
     * all interfaces it declares were already resolved, so walking {@link Class#getInterfaces()}
     * never triggers a fresh classload of {@code GenericRecord}.
     */
    public static boolean isGenericRecord(Class<?> clazz) {
        return GENERIC_RECORD_CACHE.computeIfAbsent(clazz, AvroStateUtils::computeIsGenericRecord);
    }

    /**
     * Does the actual interface-hierarchy walk for {@link #isGenericRecord}. Recurses into itself
     * rather than back into {@link #isGenericRecord}: {@code ConcurrentHashMap.computeIfAbsent}
     * forbids its mapping function from calling back into the same map - even for a different key -
     * and will throw {@code IllegalStateException("Recursive update")} if it does.
     */
    private static boolean computeIsGenericRecord(Class<?> clazz) {
        for (Class<?> current = clazz; current != null; current = current.getSuperclass()) {
            for (Class<?> iface : current.getInterfaces()) {
                if (iface.getName().equals(GENERIC_RECORD_CLASS_NAME)
                        || computeIsGenericRecord(iface)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Reads a field from an Avro {@code GenericRecord}. */
    public static Object getGenericRecordField(Object record, String fieldName) {
        return ((GenericRecord) record).get(fieldName);
    }
}
