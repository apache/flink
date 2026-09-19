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

package org.apache.flink.state.api.input.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CustomRestoreSerializerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.state.api.schema.AvroStateUtils;

/**
 * The {@link CustomRestoreSerializerFactory} implementation registered by the State Processing API
 * while reading state whose original classes are not on the classpath.
 *
 * <p>Dispatches on the kind of {@link TypeSerializerSnapshot} that could not resolve its class:
 *
 * <ul>
 *   <li>{@link PojoSerializerSnapshot} → a {@link PojoToRowDataDeserializer} that reads the POJO
 *       binary format directly into {@code RowData}.
 *   <li>{@link EnumSerializer.EnumSerializerSnapshot} → an {@link EnumNameDeserializer} that maps
 *       the serialized ordinal back to its constant name.
 *   <li>{@code AvroSerializerSnapshot} → an {@code AvroSerializer} reading into {@code
 *       GenericRecord}, using the schema embedded in the snapshot.
 * </ul>
 *
 * <p>See {@link CustomRestoreSerializerFactory} for why this must only ever be registered by the
 * State Processing API's own read path, never for regular job restores.
 *
 * <p>flink-avro is an optional dependency of this module (see the module {@code pom.xml}), so this
 * class must never mention an Avro type directly: doing so - even in an {@code instanceof} branch
 * that is never taken - would make the JVM try to resolve that type the moment this method is
 * reached for *any* unrecognized snapshot, throwing {@code NoClassDefFoundError} for callers who
 * never use Avro at all. The Avro-specific branch is instead selected by class name and delegated
 * to {@link AvroStateUtils}, which is only ever loaded once that name comparison has already
 * confirmed Avro is genuinely on the classpath.
 */
@Internal
public final class MissingClassSerializerFactory {

    private MissingClassSerializerFactory() {}

    public static TypeSerializer<?> create(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot instanceof PojoSerializerSnapshot) {
            return PojoToRowDataDeserializer.create((PojoSerializerSnapshot<?>) snapshot);
        }
        if (snapshot instanceof EnumSerializer.EnumSerializerSnapshot) {
            return EnumNameDeserializer.create((EnumSerializer.EnumSerializerSnapshot<?>) snapshot);
        }
        if (AvroStateUtils.AVRO_SERIALIZER_SNAPSHOT_CLASS_NAME.equals(
                snapshot.getClass().getName())) {
            return AvroStateUtils.createFallbackSerializer(snapshot);
        }
        throw new UnsupportedOperationException(
                "No fallback serializer available for snapshot of type '"
                        + snapshot.getClass().getName()
                        + "'.");
    }
}
