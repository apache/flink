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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.IntermediateCompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.LinkedOptionalMap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Snapshot class for the {@link PojoSerializer}. */
@Internal
public class PojoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    /**
     * We start from version {@code 2}. {@code 1} is retained for {@link
     * PojoSerializer.PojoSerializerConfigSnapshot}.
     */
    private static final int VERSION = 2;

    /** Contains the actual content for the serializer snapshot. */
    private PojoSerializerSnapshotData<T> snapshotData;

    /** Constructor for reading the snapshot. */
    public PojoSerializerSnapshot() {}

    /**
     * Constructor for writing the snapshot.
     *
     * @param pojoClass the Pojo type class.
     * @param fields array of fields. Fields may be {@code null} if the originating {@link
     *     PojoSerializer} is a restored one with already missing fields, and was never replaced by
     *     a new {@link PojoSerializer}.
     * @param fieldSerializers array of field serializers.
     * @param registeredSubclassSerializers map of registered subclasses to their corresponding
     *     serializers.
     * @param nonRegisteredSubclassSerializers map of non-registered subclasses to their
     *     corresponding serializers.
     */
    PojoSerializerSnapshot(
            Class<T> pojoClass,
            Field[] fields,
            TypeSerializer<?>[] fieldSerializers,
            LinkedHashMap<Class<?>, TypeSerializer<?>> registeredSubclassSerializers,
            Map<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers) {

        this.snapshotData =
                PojoSerializerSnapshotData.createFrom(
                        pojoClass,
                        fields,
                        fieldSerializers,
                        registeredSubclassSerializers,
                        nonRegisteredSubclassSerializers);
    }

    /**
     * Constructor for backwards compatibility paths with the {@link
     * PojoSerializer.PojoSerializerConfigSnapshot}. This is used in {@link
     * PojoSerializer.PojoSerializerConfigSnapshot#resolveSchemaCompatibility(TypeSerializer)} to
     * delegate the compatibility check to this snapshot class.
     */
    PojoSerializerSnapshot(
            Class<T> pojoClass,
            Field[] fields,
            TypeSerializerSnapshot<?>[] existingFieldSerializerSnapshots,
            LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>>
                    existingRegisteredSubclassSerializerSnapshots,
            Map<Class<?>, TypeSerializerSnapshot<?>>
                    existingNonRegisteredSubclassSerializerSnapshots) {

        this.snapshotData =
                PojoSerializerSnapshotData.createFrom(
                        pojoClass,
                        fields,
                        existingFieldSerializerSnapshots,
                        existingRegisteredSubclassSerializerSnapshots,
                        existingNonRegisteredSubclassSerializerSnapshots);
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
        checkArgument(readVersion == 2, "unrecognized read version %s", readVersion);
        snapshotData = PojoSerializerSnapshotData.createFrom(in, userCodeClassLoader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<T> restoreSerializer() {
        final int numFields = snapshotData.getFieldSerializerSnapshots().size();

        final ArrayList<Field> restoredFields = new ArrayList<>(numFields);
        final ArrayList<TypeSerializer<?>> restoredFieldSerializers = new ArrayList<>(numFields);
        snapshotData
                .getFieldSerializerSnapshots()
                .forEach(
                        (fieldName, field, fieldSerializerSnapshot) -> {
                            restoredFields.add(field);
                            checkState(
                                    fieldSerializerSnapshot != null,
                                    "field serializer snapshots should be present.");
                            restoredFieldSerializers.add(
                                    fieldSerializerSnapshot.restoreSerializer());
                        });

        final LinkedHashMap<Class<?>, TypeSerializer<?>> registeredSubclassSerializers =
                restoreSerializers(
                        snapshotData.getRegisteredSubclassSerializerSnapshots().unwrapOptionals());
        final Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]>
                decomposedSubclassSerializerRegistry =
                        decomposeSubclassSerializerRegistry(registeredSubclassSerializers);

        final LinkedHashMap<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers =
                restoreSerializers(
                        snapshotData
                                .getNonRegisteredSubclassSerializerSnapshots()
                                .unwrapOptionals());

        return new PojoSerializer<>(
                snapshotData.getPojoClass(),
                restoredFields.toArray(new Field[numFields]),
                restoredFieldSerializers.toArray(new TypeSerializer[numFields]),
                decomposedSubclassSerializerRegistry.f0,
                decomposedSubclassSerializerRegistry.f1,
                nonRegisteredSubclassSerializers,
                new ExecutionConfig());
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (newSerializer.getClass() != PojoSerializer.class) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        final PojoSerializer<T> newPojoSerializer = (PojoSerializer<T>) newSerializer;

        final Class<T> previousPojoClass = snapshotData.getPojoClass();
        final LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots =
                snapshotData.getFieldSerializerSnapshots();
        final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                registeredSubclassSerializerSnapshots =
                        snapshotData.getRegisteredSubclassSerializerSnapshots();
        final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                nonRegisteredSubclassSerializerSnapshots =
                        snapshotData.getNonRegisteredSubclassSerializerSnapshots();

        if (previousPojoClass != newPojoSerializer.getPojoClass()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (registeredSubclassSerializerSnapshots.hasAbsentKeysOrValues()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (nonRegisteredSubclassSerializerSnapshots.hasAbsentKeysOrValues()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        final IntermediateCompatibilityResult<T> preExistingFieldSerializersCompatibility =
                getCompatibilityOfPreExistingFields(newPojoSerializer, fieldSerializerSnapshots);

        if (preExistingFieldSerializersCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        final IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility =
                getCompatibilityOfPreExistingRegisteredSubclasses(
                        newPojoSerializer, registeredSubclassSerializerSnapshots);

        if (preExistingRegistrationsCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (newPojoSerializerIsCompatibleAfterMigration(
                newPojoSerializer,
                preExistingFieldSerializersCompatibility,
                preExistingRegistrationsCompatibility,
                fieldSerializerSnapshots)) {

            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        if (newPojoSerializerIsCompatibleWithReconfiguredSerializer(
                newPojoSerializer,
                preExistingFieldSerializersCompatibility,
                preExistingRegistrationsCompatibility,
                registeredSubclassSerializerSnapshots,
                nonRegisteredSubclassSerializerSnapshots)) {

            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    constructReconfiguredPojoSerializer(
                            newPojoSerializer,
                            preExistingFieldSerializersCompatibility,
                            registeredSubclassSerializerSnapshots,
                            preExistingRegistrationsCompatibility,
                            nonRegisteredSubclassSerializerSnapshots));
        }

        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    // ---------------------------------------------------------------------------------------------
    //  Utility methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Transforms a {@link LinkedHashMap} with {@link TypeSerializerSnapshot}s as the value to
     * {@link TypeSerializer} as the value by restoring the snapshot.
     */
    private static <K> LinkedHashMap<K, TypeSerializer<?>> restoreSerializers(
            LinkedHashMap<K, TypeSerializerSnapshot<?>> snapshotsMap) {
        final LinkedHashMap<K, TypeSerializer<?>> restoredSerializersMap =
                new LinkedHashMap<>(snapshotsMap.size());
        snapshotsMap.forEach(
                (key, snapshot) -> restoredSerializersMap.put(key, snapshot.restoreSerializer()));
        return restoredSerializersMap;
    }

    /**
     * Transforms the subclass serializer registry structure, {@code LinkedHashMap<Class<?>,
     * TypeSerializer<?>>} to 2 separate structures: a map containing with registered classes as key
     * and their corresponding ids (order in the original map) as value, as well as a separate array
     * of the corresponding subclass serializers.
     */
    @SuppressWarnings("unchecked")
    private static Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]>
            decomposeSubclassSerializerRegistry(
                    LinkedHashMap<Class<?>, TypeSerializer<?>> subclassSerializerRegistry) {

        final LinkedHashMap<Class<?>, Integer> subclassIds =
                new LinkedHashMap<>(subclassSerializerRegistry.size());
        final TypeSerializer[] subclassSerializers =
                new TypeSerializer[subclassSerializerRegistry.size()];

        subclassSerializerRegistry.forEach(
                (registeredSubclassClass, serializer) -> {
                    int id = subclassIds.size();
                    subclassIds.put(registeredSubclassClass, id);
                    subclassSerializers[id] = serializer;
                });

        return Tuple2.of(subclassIds, subclassSerializers);
    }

    /**
     * Finds which Pojo fields exists both in the new {@link PojoSerializer} as well as in the
     * previous one (represented by this snapshot), and returns an {@link
     * IntermediateCompatibilityResult} of the serializers of those preexisting fields.
     */
    private static <T> IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingFields(
            PojoSerializer<T> newPojoSerializer,
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots) {

        // the present entries dictates the preexisting fields, because removed fields would be
        // represented as absent keys in the optional map.
        final Set<LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>>>
                presentFieldSnapshots = fieldSerializerSnapshots.getPresentEntries();

        final ArrayList<TypeSerializerSnapshot<?>> associatedFieldSerializerSnapshots =
                new ArrayList<>(presentFieldSnapshots.size());
        final ArrayList<TypeSerializer<?>> associatedNewFieldSerializers =
                new ArrayList<>(presentFieldSnapshots.size());

        final Map<Field, TypeSerializer<?>> newFieldSerializersIndex =
                buildNewFieldSerializersIndex(newPojoSerializer);
        for (LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>> presentFieldEntry :
                presentFieldSnapshots) {
            TypeSerializer<?> associatedNewFieldSerializer =
                    newFieldSerializersIndex.get(presentFieldEntry.getKey());
            checkState(
                    associatedNewFieldSerializer != null,
                    "a present field should have its associated new field serializer available.");

            associatedFieldSerializerSnapshots.add(presentFieldEntry.getValue());
            associatedNewFieldSerializers.add(associatedNewFieldSerializer);
        }

        return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                associatedNewFieldSerializers.toArray(
                        new TypeSerializer<?>[associatedNewFieldSerializers.size()]),
                associatedFieldSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>[associatedFieldSerializerSnapshots.size()]));
    }

    /**
     * Builds an index of fields to their corresponding serializers for the new {@link
     * PojoSerializer} for faster field serializer lookups.
     */
    private static <T> Map<Field, TypeSerializer<?>> buildNewFieldSerializersIndex(
            PojoSerializer<T> newPojoSerializer) {
        final Field[] newFields = newPojoSerializer.getFields();
        final TypeSerializer<?>[] newFieldSerializers = newPojoSerializer.getFieldSerializers();

        checkState(newFields.length == newFieldSerializers.length);

        int numFields = newFields.length;
        final Map<Field, TypeSerializer<?>> index = new HashMap<>(numFields);
        for (int i = 0; i < numFields; i++) {
            index.put(newFields[i], newFieldSerializers[i]);
        }

        return index;
    }

    /**
     * Finds which registered subclasses exists both in the new {@link PojoSerializer} as well as in
     * the previous one (represented by this snapshot), and returns an {@link
     * IntermediateCompatibilityResult} of the serializers of this preexisting registered
     * subclasses.
     */
    private static <T>
            IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingRegisteredSubclasses(
                    PojoSerializer<T> newPojoSerializer,
                    LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                            registeredSubclassSerializerSnapshots) {

        final LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> unwrappedSerializerSnapshots =
                registeredSubclassSerializerSnapshots.unwrapOptionals();

        final ArrayList<TypeSerializerSnapshot<?>> associatedSubclassSerializerSnapshots =
                new ArrayList<>();
        final ArrayList<TypeSerializer<?>> associatedNewSubclassSerializers = new ArrayList<>();

        final LinkedHashMap<Class<?>, TypeSerializer<?>> newSubclassSerializerRegistry =
                newPojoSerializer.getBundledSubclassSerializerRegistry();

        for (Map.Entry<Class<?>, TypeSerializerSnapshot<?>> entry :
                unwrappedSerializerSnapshots.entrySet()) {
            TypeSerializer<?> newRegisteredSerializer =
                    newSubclassSerializerRegistry.get(entry.getKey());
            if (newRegisteredSerializer != null) {
                associatedSubclassSerializerSnapshots.add(entry.getValue());
                associatedNewSubclassSerializers.add(newRegisteredSerializer);
            }
        }

        return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                associatedNewSubclassSerializers.toArray(
                        new TypeSerializer<?>[associatedNewSubclassSerializers.size()]),
                associatedSubclassSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>
                                [associatedSubclassSerializerSnapshots.size()]));
    }

    /** Checks if the new {@link PojoSerializer} is compatible after migration. */
    private static <T> boolean newPojoSerializerIsCompatibleAfterMigration(
            PojoSerializer<T> newPojoSerializer,
            IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
            IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots) {
        return newPojoHasNewOrRemovedFields(fieldSerializerSnapshots, newPojoSerializer)
                || fieldSerializerCompatibility.isCompatibleAfterMigration()
                || preExistingRegistrationsCompatibility.isCompatibleAfterMigration();
    }

    /** Checks if the new {@link PojoSerializer} is compatible with a reconfigured instance. */
    private static <T> boolean newPojoSerializerIsCompatibleWithReconfiguredSerializer(
            PojoSerializer<T> newPojoSerializer,
            IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
            IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    registeredSubclassSerializerSnapshots,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    nonRegisteredSubclassSerializerSnapshots) {
        return newPojoHasDifferentSubclassRegistrationOrder(
                        registeredSubclassSerializerSnapshots, newPojoSerializer)
                || previousSerializerHasNonRegisteredSubclasses(
                        nonRegisteredSubclassSerializerSnapshots)
                || fieldSerializerCompatibility.isCompatibleWithReconfiguredSerializer()
                || preExistingRegistrationsCompatibility.isCompatibleWithReconfiguredSerializer();
    }

    /**
     * Checks whether the new {@link PojoSerializer} has new or removed fields compared to the
     * previous one.
     */
    private static boolean newPojoHasNewOrRemovedFields(
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots,
            PojoSerializer<?> newPojoSerializer) {
        int numRemovedFields = fieldSerializerSnapshots.absentKeysOrValues().size();
        int numPreexistingFields = fieldSerializerSnapshots.size() - numRemovedFields;

        boolean hasRemovedFields = numRemovedFields > 0;
        boolean hasNewFields = newPojoSerializer.getFields().length - numPreexistingFields > 0;
        return hasRemovedFields || hasNewFields;
    }

    /**
     * Checks whether the new {@link PojoSerializer} has a different subclass registration order
     * compared to the previous one.
     */
    private static boolean newPojoHasDifferentSubclassRegistrationOrder(
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    registeredSubclassSerializerSnapshots,
            PojoSerializer<?> newPojoSerializer) {
        Set<Class<?>> previousRegistrationOrder =
                registeredSubclassSerializerSnapshots.unwrapOptionals().keySet();
        Set<Class<?>> newRegistrationOrder = newPojoSerializer.getRegisteredClasses().keySet();
        return !isPreviousRegistrationPrefixOfNewRegistration(
                previousRegistrationOrder, newRegistrationOrder);
    }

    private static boolean isPreviousRegistrationPrefixOfNewRegistration(
            Set<Class<?>> previousRegistrationOrder, Set<Class<?>> newRegistrationOrder) {
        Iterator<Class<?>> newRegistrationItr = newRegistrationOrder.iterator();

        for (Class<?> previousRegisteredClass : previousRegistrationOrder) {
            if (!newRegistrationItr.hasNext()) {
                return false;
            }
            Class<?> newRegisteredClass = newRegistrationItr.next();
            if (!previousRegisteredClass.equals(newRegisteredClass)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether the previous serializer, represented by this snapshot, has non-registered
     * subclasses.
     */
    private static boolean previousSerializerHasNonRegisteredSubclasses(
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    nonRegisteredSubclassSerializerSnapshots) {
        return nonRegisteredSubclassSerializerSnapshots.size() > 0;
    }

    /**
     * Creates a reconfigured version of the {@link PojoSerializer}.
     *
     * @param originalNewPojoSerializer the original new {@link PojoSerializer} to create a
     *     reconfigured version of.
     * @param fieldSerializerCompatibility compatibility of preexisting fields' serializers.
     * @param registeredSerializerSnapshots snapshot of previous registered subclasses' serializers.
     * @param preExistingRegistrationsCompatibility compatibility of preexisting subclasses'
     *     serializers.
     * @param nonRegisteredSubclassSerializerSnapshots snapshot of previous non-registered
     *     subclasses' serializers.
     * @return a reconfigured version of the original new {@link PojoSerializer}.
     */
    private static <T> PojoSerializer<T> constructReconfiguredPojoSerializer(
            PojoSerializer<T> originalNewPojoSerializer,
            IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSerializerSnapshots,
            IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    nonRegisteredSubclassSerializerSnapshots) {

        @SuppressWarnings("unchecked")
        final TypeSerializer<Object>[] reconfiguredFieldSerializers =
                constructReconfiguredFieldSerializers(fieldSerializerCompatibility);

        Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]>
                reconfiguredSubclassRegistry =
                        constructReconfiguredSubclassRegistry(
                                originalNewPojoSerializer.getBundledSubclassSerializerRegistry(),
                                registeredSerializerSnapshots,
                                preExistingRegistrationsCompatibility);

        return new PojoSerializer<>(
                originalNewPojoSerializer.getPojoClass(),
                originalNewPojoSerializer.getFields(),
                reconfiguredFieldSerializers,
                reconfiguredSubclassRegistry.f0,
                reconfiguredSubclassRegistry.f1,
                restoreSerializers(nonRegisteredSubclassSerializerSnapshots.unwrapOptionals()),
                originalNewPojoSerializer.getExecutionConfig());
    }

    private static TypeSerializer[] constructReconfiguredFieldSerializers(
            IntermediateCompatibilityResult<?> fieldSerializerCompatibility) {
        checkArgument(
                !fieldSerializerCompatibility.isIncompatible()
                        && !fieldSerializerCompatibility.isCompatibleAfterMigration());
        return fieldSerializerCompatibility.getNestedSerializers();
    }

    private static Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]>
            constructReconfiguredSubclassRegistry(
                    LinkedHashMap<Class<?>, TypeSerializer<?>> newSubclassRegistrations,
                    LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                            registeredSerializerSnapshots,
                    IntermediateCompatibilityResult<?> preExistingRegistrationsCompatibility) {

        checkArgument(
                !preExistingRegistrationsCompatibility.isIncompatible()
                        && !preExistingRegistrationsCompatibility.isCompatibleAfterMigration());

        LinkedHashMap<Class<?>, TypeSerializer<?>> reconfiguredSubclassSerializerRegistry =
                restoreSerializers(registeredSerializerSnapshots.unwrapOptionals());

        Iterator<TypeSerializer<?>> serializersForPreexistingRegistrations =
                Arrays.asList(preExistingRegistrationsCompatibility.getNestedSerializers())
                        .iterator();

        // first, replace all restored serializers of subclasses that co-exist in
        // the previous and new registrations, with the compatibility-checked serializers
        for (Map.Entry<Class<?>, TypeSerializer<?>> oldRegistration :
                reconfiguredSubclassSerializerRegistry.entrySet()) {
            if (newSubclassRegistrations.containsKey(oldRegistration.getKey())) {
                oldRegistration.setValue(serializersForPreexistingRegistrations.next());
            }
        }

        // then, for all new registration that did not exist before, append it to the registry
        // simply with their
        // new serializers
        for (Map.Entry<Class<?>, TypeSerializer<?>> newRegistration :
                newSubclassRegistrations.entrySet()) {
            TypeSerializer<?> oldRegistration =
                    reconfiguredSubclassSerializerRegistry.get(newRegistration.getKey());
            if (oldRegistration == null) {
                reconfiguredSubclassSerializerRegistry.put(
                        newRegistration.getKey(), newRegistration.getValue());
            }
        }

        return decomposeSubclassSerializerRegistry(reconfiguredSubclassSerializerRegistry);
    }
}
