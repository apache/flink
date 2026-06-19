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
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.IntermediateCompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.LinkedOptionalMap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
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

    /**
     * Configuration of the current execution. WARN: it's just used as point-in-time view to check
     * or generate something which should never be used in writeSnapshot or readSnapshot.
     */
    private SerializerConfig serializerConfig;

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
            Map<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers,
            SerializerConfig serializerConfig) {

        this.snapshotData =
                PojoSerializerSnapshotData.createFrom(
                        pojoClass,
                        fields,
                        fieldSerializers,
                        registeredSubclassSerializers,
                        nonRegisteredSubclassSerializers);
        this.serializerConfig = serializerConfig;
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
                    existingNonRegisteredSubclassSerializerSnapshots,
            SerializerConfig serializerConfig) {

        this.snapshotData =
                PojoSerializerSnapshotData.createFrom(
                        pojoClass,
                        fields,
                        existingFieldSerializerSnapshots,
                        existingRegisteredSubclassSerializerSnapshots,
                        existingNonRegisteredSubclassSerializerSnapshots);
        this.serializerConfig = serializerConfig;
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
                new SerializerConfigImpl());
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSerializerSnapshot) {
        if (!(oldSerializerSnapshot instanceof PojoSerializerSnapshot)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        PojoSerializerSnapshot<T> previousPojoSerializerSnapshot =
                (PojoSerializerSnapshot<T>) oldSerializerSnapshot;

        final Class<T> previousPojoClass =
                previousPojoSerializerSnapshot.snapshotData.getPojoClass();
        final LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots =
                previousPojoSerializerSnapshot.snapshotData.getFieldSerializerSnapshots();
        final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                registeredSubclassSerializerSnapshots =
                        previousPojoSerializerSnapshot.snapshotData
                                .getRegisteredSubclassSerializerSnapshots();
        final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                nonRegisteredSubclassSerializerSnapshots =
                        previousPojoSerializerSnapshot.snapshotData
                                .getNonRegisteredSubclassSerializerSnapshots();

        if (previousPojoClass != snapshotData.getPojoClass()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (registeredSubclassSerializerSnapshots.hasAbsentKeysOrValues()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (nonRegisteredSubclassSerializerSnapshots.hasAbsentKeysOrValues()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        final IntermediateCompatibilityResult<T> preExistingFieldSerializersCompatibility =
                getCompatibilityOfPreExistingFields(fieldSerializerSnapshots);

        if (preExistingFieldSerializersCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        final IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility =
                getCompatibilityOfPreExistingRegisteredSubclasses(
                        registeredSubclassSerializerSnapshots);

        if (preExistingRegistrationsCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (newPojoSerializerIsCompatibleAfterMigration(
                preExistingFieldSerializersCompatibility,
                preExistingRegistrationsCompatibility,
                fieldSerializerSnapshots)) {

            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        if (newPojoSerializerIsCompatibleWithReconfiguredSerializer(
                preExistingFieldSerializersCompatibility,
                preExistingRegistrationsCompatibility,
                registeredSubclassSerializerSnapshots,
                nonRegisteredSubclassSerializerSnapshots)) {

            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    constructReconfiguredPojoSerializer(
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
                CollectionUtil.newLinkedHashMapWithExpectedSize(snapshotsMap.size());
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
                CollectionUtil.newLinkedHashMapWithExpectedSize(subclassSerializerRegistry.size());
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
     * Finds which Pojo fields exists both in the new {@link PojoSerializerSnapshot} as well as in
     * the previous one, and returns an {@link IntermediateCompatibilityResult} of the serializers
     * of those preexisting fields.
     */
    private IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingFields(
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> oldFieldSerializerSnapshots) {

        // the present entries dictates the preexisting fields, because removed fields would be
        // represented as absent keys in the optional map.
        final Set<LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>>>
                presentFieldSnapshots = oldFieldSerializerSnapshots.getPresentEntries();

        final ArrayList<TypeSerializerSnapshot<?>> associatedFieldSerializerSnapshots =
                new ArrayList<>(presentFieldSnapshots.size());
        final ArrayList<TypeSerializerSnapshot<?>> associatedNewFieldSerializerSnapshots =
                new ArrayList<>(presentFieldSnapshots.size());

        Map<Field, TypeSerializerSnapshot<?>> newFieldSerializerSnapshots =
                snapshotData.getFieldSerializerSnapshots().unwrapOptionals();
        for (LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>> presentFieldEntry :
                presentFieldSnapshots) {
            TypeSerializerSnapshot<?> associatedNewFieldSerializer =
                    newFieldSerializerSnapshots.get(presentFieldEntry.getKey());
            checkState(
                    associatedNewFieldSerializer != null,
                    "a present field should have its associated new field serializer available.");

            associatedFieldSerializerSnapshots.add(presentFieldEntry.getValue());
            associatedNewFieldSerializerSnapshots.add(associatedNewFieldSerializer);
        }

        return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                associatedNewFieldSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>
                                [associatedNewFieldSerializerSnapshots.size()]),
                associatedFieldSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>[associatedFieldSerializerSnapshots.size()]));
    }

    /**
     * Finds which registered subclasses exists both in the new {@link PojoSerializerSnapshot} as
     * well as in the previous one, and returns an {@link IntermediateCompatibilityResult} of the
     * serializers of this preexisting registered subclasses.
     */
    private IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingRegisteredSubclasses(
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    registeredSubclassSerializerSnapshots) {

        final LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> unwrappedSerializerSnapshots =
                registeredSubclassSerializerSnapshots.unwrapOptionals();

        final ArrayList<TypeSerializerSnapshot<?>> associatedSubclassSerializerSnapshots =
                new ArrayList<>();
        final ArrayList<TypeSerializerSnapshot<?>> associatedNewSubclassSerializerSnapshots =
                new ArrayList<>();

        final LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> newSubclassSerializerRegistry =
                snapshotData.getRegisteredSubclassSerializerSnapshots().unwrapOptionals();

        for (Map.Entry<Class<?>, TypeSerializerSnapshot<?>> entry :
                unwrappedSerializerSnapshots.entrySet()) {
            TypeSerializerSnapshot<?> newRegisteredSerializerSnapshot =
                    newSubclassSerializerRegistry.get(entry.getKey());
            if (newRegisteredSerializerSnapshot != null) {
                associatedSubclassSerializerSnapshots.add(entry.getValue());
                associatedNewSubclassSerializerSnapshots.add(newRegisteredSerializerSnapshot);
            }
        }

        return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                associatedNewSubclassSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>
                                [associatedNewSubclassSerializerSnapshots.size()]),
                associatedSubclassSerializerSnapshots.toArray(
                        new TypeSerializerSnapshot<?>
                                [associatedSubclassSerializerSnapshots.size()]));
    }

    /** Checks if the new {@link PojoSerializerSnapshot} is compatible after migration. */
    private boolean newPojoSerializerIsCompatibleAfterMigration(
            IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
            IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots) {
        return newPojoHasNewOrRemovedFields(fieldSerializerSnapshots)
                || fieldSerializerCompatibility.isCompatibleAfterMigration()
                || preExistingRegistrationsCompatibility.isCompatibleAfterMigration();
    }

    /**
     * Checks if the new {@link PojoSerializerSnapshot} is compatible with a reconfigured instance.
     */
    private boolean newPojoSerializerIsCompatibleWithReconfiguredSerializer(
            IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
            IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    registeredSubclassSerializerSnapshots,
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    nonRegisteredSubclassSerializerSnapshots) {
        return newPojoHasDifferentSubclassRegistrationOrder(registeredSubclassSerializerSnapshots)
                || previousSerializerHasNonRegisteredSubclasses(
                        nonRegisteredSubclassSerializerSnapshots)
                || fieldSerializerCompatibility.isCompatibleWithReconfiguredSerializer()
                || preExistingRegistrationsCompatibility.isCompatibleWithReconfiguredSerializer();
    }

    /**
     * Checks whether the new {@link PojoSerializerSnapshot} has new or removed fields compared to
     * the previous one.
     */
    private boolean newPojoHasNewOrRemovedFields(
            LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots) {
        int numRemovedFields = fieldSerializerSnapshots.absentKeysOrValues().size();
        int numPreexistingFields = fieldSerializerSnapshots.size() - numRemovedFields;

        boolean hasRemovedFields = numRemovedFields > 0;
        boolean hasNewFields =
                snapshotData.getFieldSerializerSnapshots().size() - numPreexistingFields > 0;
        return hasRemovedFields || hasNewFields;
    }

    /**
     * Checks whether the new {@link PojoSerializerSnapshot} has a different subclass registration
     * order compared to the previous one.
     */
    private boolean newPojoHasDifferentSubclassRegistrationOrder(
            LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                    registeredSubclassSerializerSnapshots) {
        Set<Class<?>> previousRegistrationOrder =
                registeredSubclassSerializerSnapshots.unwrapOptionals().keySet();
        Set<Class<?>> newRegistrationOrder =
                snapshotData.getRegisteredSubclassSerializerSnapshots().unwrapOptionals().keySet();
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
     * Creates a reconfigured version of the {@link PojoSerializerSnapshot}.
     *
     * @param fieldSerializerCompatibility compatibility of preexisting fields' serializers.
     * @param registeredSerializerSnapshots snapshot of previous registered subclasses' serializers.
     * @param preExistingRegistrationsCompatibility compatibility of preexisting subclasses'
     *     serializers.
     * @param nonRegisteredSubclassSerializerSnapshots snapshot of previous non-registered
     *     subclasses' serializers.
     * @return a reconfigured version of the original new {@link PojoSerializer}.
     */
    private PojoSerializer<T> constructReconfiguredPojoSerializer(
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
                        constructReconfiguredSubclassRegistrations(
                                snapshotData
                                        .getRegisteredSubclassSerializerSnapshots()
                                        .unwrapOptionals(),
                                registeredSerializerSnapshots,
                                preExistingRegistrationsCompatibility);

        return new PojoSerializer<>(
                snapshotData.getPojoClass(),
                snapshotData
                        .getFieldSerializerSnapshots()
                        .unwrapOptionals()
                        .keySet()
                        .toArray(new Field[0]),
                reconfiguredFieldSerializers,
                reconfiguredSubclassRegistry.f0,
                reconfiguredSubclassRegistry.f1,
                restoreSerializers(nonRegisteredSubclassSerializerSnapshots.unwrapOptionals()),
                serializerConfig);
    }

    private static TypeSerializer[] constructReconfiguredFieldSerializers(
            IntermediateCompatibilityResult<?> fieldSerializerCompatibility) {
        checkArgument(
                !fieldSerializerCompatibility.isIncompatible()
                        && !fieldSerializerCompatibility.isCompatibleAfterMigration());
        return fieldSerializerCompatibility.getNestedSerializers();
    }

    private static Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]>
            constructReconfiguredSubclassRegistrations(
                    LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> newSubclassRegistrations,
                    LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>>
                            oldRegisteredSerializerSnapshots,
                    IntermediateCompatibilityResult<?> preExistingRegistrationsCompatibility) {

        checkArgument(
                !preExistingRegistrationsCompatibility.isIncompatible()
                        && !preExistingRegistrationsCompatibility.isCompatibleAfterMigration());

        LinkedHashMap<Class<?>, TypeSerializer<?>> reconfiguredSubclassSerializerRegistry =
                restoreSerializers(oldRegisteredSerializerSnapshots.unwrapOptionals());

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
        for (Map.Entry<Class<?>, TypeSerializerSnapshot<?>> newRegistration :
                newSubclassRegistrations.entrySet()) {
            TypeSerializer<?> oldRegistration =
                    reconfiguredSubclassSerializerRegistry.get(newRegistration.getKey());
            if (oldRegistration == null) {
                reconfiguredSubclassSerializerRegistry.put(
                        newRegistration.getKey(), newRegistration.getValue().restoreSerializer());
            }
        }

        return decomposeSubclassSerializerRegistry(reconfiguredSubclassSerializerRegistry);
    }
}
