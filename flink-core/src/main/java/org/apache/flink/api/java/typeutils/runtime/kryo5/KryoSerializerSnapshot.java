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

package org.apache.flink.api.java.typeutils.runtime.kryo5;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig.SerializableKryo5Serializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.LinkedOptionalMap;
import org.apache.flink.util.LinkedOptionalMap.MergeResult;

import com.esotericsoftware.kryo.kryo5.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializerSnapshotData.createFrom;
import static org.apache.flink.util.LinkedOptionalMap.mergeRightIntoLeft;
import static org.apache.flink.util.LinkedOptionalMap.optionalMapOf;

/** {@link TypeSerializerSnapshot} for {@link KryoSerializer}. */
@PublicEvolving
public class KryoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KryoSerializerSnapshot.class);

    private static final int VERSION = 2;

    private KryoSerializerSnapshotData<T> snapshotData;

    @SuppressWarnings("unused")
    public KryoSerializerSnapshot() {}

    KryoSerializerSnapshot(
            Class<T> typeClass,
            LinkedHashMap<Class<?>, SerializableKryo5Serializer<?>> defaultKryoSerializers,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
            LinkedHashMap<String, Kryo5Registration> kryoRegistrations) {

        this.snapshotData =
                createFrom(
                        typeClass,
                        defaultKryoSerializers,
                        defaultKryoSerializerClasses,
                        kryoRegistrations);
    }

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        snapshotData.writeSnapshotData(out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        this.snapshotData = createFrom(in, userCodeClassLoader);
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new KryoSerializer<>(
                snapshotData.getTypeClass(),
                snapshotData.getDefaultKryoSerializers().unwrapOptionals(),
                snapshotData.getDefaultKryoSerializerClasses().unwrapOptionals(),
                snapshotData.getKryoRegistrations().unwrapOptionals(),
                null);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (newSerializer instanceof KryoSerializer) {
            KryoSerializer<T> kryoSerializer = (KryoSerializer<T>) newSerializer;
            if (kryoSerializer.getType() != snapshotData.getTypeClass()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return resolveSchemaCompatibility(kryoSerializer);
        } else if (newSerializer
                instanceof org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer) {
            org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<T> kryoSerializer =
                    (org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<T>)
                            newSerializer;
            if (kryoSerializer.getType() != snapshotData.getTypeClass()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return resolveBackwardSchemaCompatibility(kryoSerializer);
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }

    private TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            KryoSerializer<T> newSerializer) {
        // merge the default serializers
        final MergeResult<Class<?>, SerializableKryo5Serializer<?>>
                reconfiguredDefaultKryoSerializers =
                        mergeRightIntoLeft(
                                snapshotData.getDefaultKryoSerializers(),
                                optionalMapOf(
                                        newSerializer.getDefaultKryoSerializers(), Class::getName));

        if (reconfiguredDefaultKryoSerializers.hasMissingKeys()) {
            logMissingKeys(reconfiguredDefaultKryoSerializers);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // merge default serializer classes
        final MergeResult<Class<?>, Class<? extends Serializer<?>>>
                reconfiguredDefaultKryoSerializerClasses =
                        mergeRightIntoLeft(
                                snapshotData.getDefaultKryoSerializerClasses(),
                                optionalMapOf(
                                        newSerializer.getDefaultKryoSerializerClasses(),
                                        Class::getName));

        if (reconfiguredDefaultKryoSerializerClasses.hasMissingKeys()) {
            logMissingKeys(reconfiguredDefaultKryoSerializerClasses);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // merge registration
        final MergeResult<String, Kryo5Registration> reconfiguredRegistrations =
                mergeRightIntoLeft(
                        snapshotData.getKryoRegistrations(),
                        optionalMapOf(newSerializer.getKryoRegistrations(), Function.identity()));

        if (reconfiguredRegistrations.hasMissingKeys()) {
            logMissingKeys(reconfiguredRegistrations);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (reconfiguredDefaultKryoSerializers.isOrderedSubset()
                && reconfiguredDefaultKryoSerializerClasses.isOrderedSubset()
                && reconfiguredRegistrations.isOrderedSubset()) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        // reconfigure a new KryoSerializer
        KryoSerializer<T> reconfiguredSerializer =
                new KryoSerializer<>(
                        snapshotData.getTypeClass(),
                        reconfiguredDefaultKryoSerializers.getMerged(),
                        reconfiguredDefaultKryoSerializerClasses.getMerged(),
                        reconfiguredRegistrations.getMerged(),
                        null);

        return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                reconfiguredSerializer);
    }

    @SuppressWarnings("deprecation")
    private TypeSerializerSchemaCompatibility<T> resolveBackwardSchemaCompatibility(
            org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<T> kryo2Serializer) {
        // Default Kryo Serializers
        LinkedOptionalMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                defaultSnapshotKryo5Serializers = snapshotData.getDefaultKryoSerializers();

        LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
                defaultKryo2SerializersRaw = kryo2Serializer.getDefaultKryoSerializers();
        Set<String> defaultKryo2SerializersClassNames =
                defaultKryo2SerializersRaw.keySet().stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        MergeResult<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                defaultKryo2SerializersMergeResult =
                        LinkedOptionalMap.mergeValuesWithPrefixKeys(
                                defaultSnapshotKryo5Serializers, defaultKryo2SerializersClassNames);

        if (defaultKryo2SerializersMergeResult.hasMissingKeys()) {
            logMissingKeys(defaultKryo2SerializersMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // Default Serializer Classes
        LinkedOptionalMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                defaultSnapshotKryo5SerializerClasses =
                        snapshotData.getDefaultKryoSerializerClasses();

        LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.Serializer<?>>>
                kryo2SerializerClassesRaw = kryo2Serializer.getDefaultKryoSerializerClasses();
        Set<String> kryo2SerializerClassesClassNames =
                kryo2SerializerClassesRaw.keySet().stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        MergeResult<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                kryoSerializersClassesMergeResult =
                        LinkedOptionalMap.mergeValuesWithPrefixKeys(
                                defaultSnapshotKryo5SerializerClasses,
                                kryo2SerializerClassesClassNames);

        if (kryoSerializersClassesMergeResult.hasMissingKeys()) {
            logMissingKeys(kryoSerializersClassesMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // Kryo Registrations
        LinkedOptionalMap<String, Kryo5Registration> snapshotKryo5Registrations =
                snapshotData.getKryoRegistrations();

        LinkedHashMap<String, KryoRegistration> kryo2RegistrationsRaw =
                kryo2Serializer.getKryoRegistrations();
        Set<String> kryo2RegistrationKeys = kryo2RegistrationsRaw.keySet();

        MergeResult<String, Kryo5Registration> kryo2RegistrationsMergeResult =
                LinkedOptionalMap.mergeValuesWithPrefixKeys(
                        snapshotKryo5Registrations, kryo2RegistrationKeys);

        if (kryo2RegistrationsMergeResult.hasMissingKeys()) {
            logMissingKeys(kryo2RegistrationsMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // reconfigure a new KryoSerializer
        KryoSerializer<T> reconfiguredSerializer =
                new KryoSerializer<>(
                        snapshotData.getTypeClass(),
                        defaultKryo2SerializersMergeResult.getMerged(),
                        kryoSerializersClassesMergeResult.getMerged(),
                        kryo2RegistrationsMergeResult.getMerged(),
                        kryo2Serializer);

        return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                reconfiguredSerializer);
    }

    private void logMissingKeys(MergeResult<?, ?> mergeResult) {
        mergeResult
                .missingKeys()
                .forEach(
                        key ->
                                LOG.warn(
                                        "The Kryo registration for a previously registered class {} does not have a "
                                                + "proper serializer, because its previous serializer cannot be loaded or is no "
                                                + "longer valid but a new serializer is not available",
                                        key));
    }
}
