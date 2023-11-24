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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.api.java.typeutils.runtime.kryo5.KryoUpgradeConfiguration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.LinkedOptionalMap;
import org.apache.flink.util.LinkedOptionalMap.MergeResult;

import com.esotericsoftware.kryo.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializerSnapshotData.createFrom;
import static org.apache.flink.util.LinkedOptionalMap.mergeRightIntoLeft;
import static org.apache.flink.util.LinkedOptionalMap.optionalMapOf;

/** {@link TypeSerializerSnapshot} for {@link KryoSerializer}. */
public class KryoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KryoSerializerSnapshot.class);

    private static final int VERSION = 2;

    private KryoSerializerSnapshotData<T> snapshotData;

    @SuppressWarnings("unused")
    public KryoSerializerSnapshot() {}

    @SuppressWarnings("deprecation")
    KryoSerializerSnapshot(
            Class<T> typeClass,
            LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
            LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

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
    @SuppressWarnings("deprecation")
    public TypeSerializer<T> restoreSerializer() {
        LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultSerializersV2 =
                snapshotData.getDefaultKryoSerializers().unwrapOptionals();
        LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClassesV2 =
                snapshotData.getDefaultKryoSerializerClasses().unwrapOptionals();
        LinkedHashMap<String, KryoRegistration> registrationsV2 =
                snapshotData.getKryoRegistrations().unwrapOptionals();

        KryoUpgradeConfiguration upgradeConfiguration = KryoUpgradeConfiguration.getActive();
        LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                defaultSerializersV5 =
                        upgradeConfiguration.upgradeDefaultSerializers(defaultSerializersV2);
        LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                defaultSerializerClassesV5 =
                        upgradeConfiguration.upgradeDefaultSerializerClasses(
                                defaultSerializerClassesV2);
        LinkedHashMap<String, Kryo5Registration> registrationsV5 =
                upgradeConfiguration.upgradeRegistrations(registrationsV2);

        return org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializer.createReturnV2(
                snapshotData.getTypeClass(),
                defaultSerializersV2,
                defaultSerializerClassesV2,
                registrationsV2,
                defaultSerializersV5,
                defaultSerializerClassesV5,
                registrationsV5);
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
                instanceof org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializer) {
            org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializer<T> kryoSerializer =
                    (org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializer<T>)
                            newSerializer;
            if (kryoSerializer.getType() != snapshotData.getTypeClass()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return resolveForwardSchemaCompatibility(kryoSerializer);
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }

    @SuppressWarnings("deprecation")
    private TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            KryoSerializer<T> newSerializer) {
        // merge the default serializers
        final MergeResult<Class<?>, SerializableSerializer<?>> reconfiguredDefaultKryoSerializers =
                mergeRightIntoLeft(
                        snapshotData.getDefaultKryoSerializers(),
                        optionalMapOf(newSerializer.getDefaultKryoSerializers(), Class::getName));

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
        final MergeResult<String, KryoRegistration> reconfiguredRegistrations =
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
    private TypeSerializerSchemaCompatibility<T> resolveForwardSchemaCompatibility(
            org.apache.flink.api.java.typeutils.runtime.kryo5.KryoSerializer<T> kryo5Serializer) {
        // Default Kryo Serializers
        LinkedOptionalMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
                defaultSnapshotKryo2Serializers = snapshotData.getDefaultKryoSerializers();

        LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                defaultKryo5SerializersRaw = kryo5Serializer.getDefaultKryoSerializers();
        Set<String> defaultKryo5SerializersClassNames =
                defaultKryo5SerializersRaw.keySet().stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        MergeResult<Class<?>, SerializableSerializer<?>> defaultKryo5SerializersMergeResult =
                LinkedOptionalMap.mergeValuesWithPrefixKeys(
                        defaultSnapshotKryo2Serializers, defaultKryo5SerializersClassNames);

        if (defaultKryo5SerializersMergeResult.hasMissingKeys()) {
            logMissingKeys(defaultKryo5SerializersMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // Default Serializer Classes
        LinkedOptionalMap<Class<?>, Class<? extends com.esotericsoftware.kryo.Serializer<?>>>
                defaultSnapshotKryo2SerializerClasses =
                        snapshotData.getDefaultKryoSerializerClasses();

        LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                kryo5SerializerClassesRaw = kryo5Serializer.getDefaultKryoSerializerClasses();
        Set<String> kryo5SerializerClassesClassNames =
                kryo5SerializerClassesRaw.keySet().stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        MergeResult<Class<?>, Class<? extends com.esotericsoftware.kryo.Serializer<?>>>
                kryoSerializersClassesMergeResult =
                        LinkedOptionalMap.mergeValuesWithPrefixKeys(
                                defaultSnapshotKryo2SerializerClasses,
                                kryo5SerializerClassesClassNames);

        if (kryoSerializersClassesMergeResult.hasMissingKeys()) {
            logMissingKeys(kryoSerializersClassesMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // Kryo Registrations
        LinkedOptionalMap<String, KryoRegistration> snapshotKryo2Registrations =
                snapshotData.getKryoRegistrations();

        LinkedHashMap<String, Kryo5Registration> kryo5RegistrationsRaw =
                kryo5Serializer.getKryoRegistrations();
        Set<String> kryo5RegistrationKeys = kryo5RegistrationsRaw.keySet();

        MergeResult<String, KryoRegistration> kryo5RegistrationsMergeResult =
                LinkedOptionalMap.mergeValuesWithPrefixKeys(
                        snapshotKryo2Registrations, kryo5RegistrationKeys);

        if (kryo5RegistrationsMergeResult.hasMissingKeys()) {
            logMissingKeys(kryo5RegistrationsMergeResult);
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // reconfigure a new KryoSerializer
        KryoSerializer<T> reconfiguredSerializer =
                new KryoSerializer<>(
                        snapshotData.getTypeClass(),
                        defaultKryo5SerializersMergeResult.getMerged(),
                        kryoSerializersClassesMergeResult.getMerged(),
                        kryo5RegistrationsMergeResult.getMerged(),
                        kryo5Serializer);

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
