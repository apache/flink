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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
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

/**
 * Snapshot class for the {@link PojoSerializer}.
 */
@Internal
public class PojoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

	/**
	 * We start from version {@code 2}. {@code 1} is retained for {@link PojoSerializer.PojoSerializerConfigSnapshot}.
	 */
	private static final int VERSION = 2;

	/**
	 * Contains the actual content for the serializer snapshot.
	 */
	private PojoSerializerSnapshotData<T> snapshotData;

	/**
	 * Constructor for reading the snapshot.
	 */
	public PojoSerializerSnapshot() {}

	/**
	 * Constructor for writing the snapshot.
	 *
	 * @param pojoClass the Pojo type class.
	 * @param fieldSerializers map of fields to their corresponding serializers.
	 * @param registeredSubclassSerializers map of registered subclasses to their corresponding serializers.
	 * @param nonRegisteredSubclassSerializers map of non-registered subclasses to their corresponding serializers.
	 */
	PojoSerializerSnapshot(
			Class<T> pojoClass,
			LinkedHashMap<Field, TypeSerializer<?>> fieldSerializers,
			LinkedHashMap<Class<?>, TypeSerializer<?>> registeredSubclassSerializers,
			HashMap<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers) {

		this.snapshotData = PojoSerializerSnapshotData.createFrom(
			pojoClass,
			fieldSerializers,
			registeredSubclassSerializers,
			nonRegisteredSubclassSerializers);
	}

	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		this.snapshotData.writeSnapshotData(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		checkArgument(readVersion == 2, "unrecognized read version %d", readVersion);
		this.snapshotData = PojoSerializerSnapshotData.createFrom(in, userCodeClassLoader);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> restoreSerializer() {
		final int numFields = snapshotData.getFieldSerializerSnapshots().size();
		final Field[] restoredFields = snapshotData.getFieldSerializerSnapshots().getKeysNoThrow().toArray(new Field[numFields]);
		final TypeSerializer<Object>[] restoredFieldSerializers = snapshotData.getFieldSerializerSnapshots().getValues()
			.stream()
			.map(TypeSerializerSnapshot::restoreSerializer)
			.toArray(TypeSerializer[]::new);

		final LinkedHashMap<Class<?>, TypeSerializer<?>> registeredSubclassSerializers = restoreSerializers(
			snapshotData.getRegisteredSubclassSerializerSnapshots().unwrapOptionals());
		final Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]> decomposedSubclassSerializerRegistry =
			decomposeSubclassSerializerRegistry(registeredSubclassSerializers);

		final LinkedHashMap<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers = restoreSerializers(
			snapshotData.getNonRegisteredSubclassSerializerSnapshots().unwrapOptionals());

		return new PojoSerializer<>(
			snapshotData.getPojoClass(),
			restoredFields,
			restoredFieldSerializers,
			decomposedSubclassSerializerRegistry.f0,
			decomposedSubclassSerializerRegistry.f1,
			nonRegisteredSubclassSerializers,
			null);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (newSerializer.getClass() != PojoSerializer.class) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		final PojoSerializer<T> newPojoSerializer = (PojoSerializer<T>) newSerializer;

		final Class<T> previousPojoClass = snapshotData.getPojoClass();
		final LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots = snapshotData.getFieldSerializerSnapshots();
		final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots = snapshotData.getRegisteredSubclassSerializerSnapshots();
		final LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots = snapshotData.getNonRegisteredSubclassSerializerSnapshots();

		if (previousPojoClass != newPojoSerializer.getPojoClass()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		if (!registeredSubclassSerializerSnapshots.absentKeysOrValues().isEmpty()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		if (!nonRegisteredSubclassSerializerSnapshots.absentKeysOrValues().isEmpty()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		final CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> fieldSerializerCompatibility =
			getCompatibilityOfPreExistingFields(newPojoSerializer, fieldSerializerSnapshots);

		final CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility =
			getCompatibilityOfPreExistingRegisteredSubclasses(newPojoSerializer, registeredSubclassSerializerSnapshots);

		if (fieldSerializerCompatibility.isIncompatible() || preExistingRegistrationsCompatibility.isIncompatible()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		if (newPojoHasNewOrRemovedFields(fieldSerializerSnapshots, newPojoSerializer)
				|| fieldSerializerCompatibility.isCompatibleAfterMigration()
				|| preExistingRegistrationsCompatibility.isCompatibleAfterMigration()) {

			return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
		} else if (newPojoHasDifferentSubclassRegistrationOrder(registeredSubclassSerializerSnapshots, newPojoSerializer)
				|| previousSerializerHasNonRegisteredSubclasses(nonRegisteredSubclassSerializerSnapshots)
				|| fieldSerializerCompatibility.isCompatibleWithReconfiguredSerializer()
				|| preExistingRegistrationsCompatibility.isCompatibleWithReconfiguredSerializer()) {

			return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
				constructReconfiguredPojoSerializer(
					newPojoSerializer,
					fieldSerializerCompatibility,
					registeredSubclassSerializerSnapshots,
					preExistingRegistrationsCompatibility,
					nonRegisteredSubclassSerializerSnapshots));
		} else {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
	}

	// ---------------------------------------------------------------------------------------------
	//  Utility methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Transforms a {@link LinkedHashMap} with {@link TypeSerializerSnapshot}s as
	 * the value to {@link TypeSerializer} as the value by restoring the snapshot.
	 */
	private static <K> LinkedHashMap<K, TypeSerializer<?>> restoreSerializers(LinkedHashMap<K, TypeSerializerSnapshot<?>> snapshotsMap) {
		final LinkedHashMap<K, TypeSerializer<?>> restoredSerializersMap = new LinkedHashMap<>(snapshotsMap.size());
		snapshotsMap.forEach((key, snapshot) -> restoredSerializersMap.put(key, snapshot.restoreSerializer()));
		return restoredSerializersMap;
	}

	/**
	 * Transforms the subclass serializer registry structure, {@code LinkedHashMap<Class<?>, TypeSerializer<?>>}
	 * to 2 separate structures: a map containing with registered classes as key and their corresponding ids (order
	 * in the original map) as value, as well as a separate array of the corresponding subclass serializers.
	 */
	@SuppressWarnings("unchecked")
	private static Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]> decomposeSubclassSerializerRegistry(
		LinkedHashMap<Class<?>, TypeSerializer<?>> subclassSerializerRegistry) {

		final LinkedHashMap<Class<?>, Integer> subclassIds = new LinkedHashMap<>(subclassSerializerRegistry.size());
		final TypeSerializer[] subclassSerializers = new TypeSerializer[subclassSerializerRegistry.size()];

		subclassSerializerRegistry.forEach((registeredSubclassClass, serializer) -> {
			int id = subclassIds.size();
			subclassIds.put(registeredSubclassClass, id);
			subclassSerializers[id] = serializer;
		});

		return Tuple2.of(subclassIds, subclassSerializers);
	}

	/**
	 * Finds which Pojo fields exists both in the new {@link PojoSerializer} as well as in the previous one
	 * (represented by this snapshot), and returns an {@link CompositeTypeSerializerUtil.IntermediateCompatibilityResult}
	 * of the serializers of those preexisting fields.
	 */
	private static <T> CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingFields(
			PojoSerializer<T> newPojoSerializer,
			LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots) {

		// the present entries dictates the preexisting fields, because removed fields would be
		// represented as absent keys in the optional map.
		final Set<LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>>> presentFieldSnapshots =
			fieldSerializerSnapshots.getPresentEntries();

		final ArrayList<TypeSerializerSnapshot<?>> associatedFieldSerializerSnapshots = new ArrayList<>(presentFieldSnapshots.size());
		final ArrayList<TypeSerializer<?>> associatedNewFieldSerializers = new ArrayList<>(presentFieldSnapshots.size());

		for (LinkedOptionalMap.KeyValue<Field, TypeSerializerSnapshot<?>> presentFieldEntry : presentFieldSnapshots) {
			TypeSerializer<?> associatedNewFieldSerializer = newPojoSerializer.getFieldSerializer(presentFieldEntry.getKey());
			checkState(
				associatedNewFieldSerializer != null,
				"a present field should have its associated new field serializer available.");

			associatedFieldSerializerSnapshots.add(presentFieldEntry.getValue());
			associatedNewFieldSerializers.add(associatedNewFieldSerializer);
		}

		return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
			associatedNewFieldSerializers.toArray(new TypeSerializer<?>[associatedNewFieldSerializers.size()]),
			associatedFieldSerializerSnapshots.toArray(new TypeSerializerSnapshot<?>[associatedFieldSerializerSnapshots.size()]));
	}

	/**
	 * Finds which registered subclasses exists both in the new {@link PojoSerializer} as well as in the previous one
	 * (represented by this snapshot), and returns an {@link CompositeTypeSerializerUtil.IntermediateCompatibilityResult}
	 * of the serializers of this preexisting registered subclasses.
	 */
	private static <T> CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> getCompatibilityOfPreExistingRegisteredSubclasses(
			PojoSerializer<T> newPojoSerializer,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots) {

		final LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> unwrappedSerializerSnapshots = registeredSubclassSerializerSnapshots.unwrapOptionals();

		final ArrayList<TypeSerializerSnapshot<?>> associatedSubclassSerializerSnapshots = new ArrayList<>();
		final ArrayList<TypeSerializer<?>> associatedNewSubclassSerializers = new ArrayList<>();

		final LinkedHashMap<Class<?>, TypeSerializer<?>> newSubclassSerializerRegistry = newPojoSerializer.getBundledSubclassSerializerRegistry();

		for (Map.Entry<Class<?>, TypeSerializerSnapshot<?>> entry : unwrappedSerializerSnapshots.entrySet()) {
			TypeSerializer<?> newRegisteredSerializer = newSubclassSerializerRegistry.get(entry.getKey());
			if (newRegisteredSerializer != null) {
				associatedSubclassSerializerSnapshots.add(entry.getValue());
				associatedNewSubclassSerializers.add(newRegisteredSerializer);
			}
		}

		return CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
			associatedNewSubclassSerializers.toArray(new TypeSerializer<?>[associatedNewSubclassSerializers.size()]),
			associatedSubclassSerializerSnapshots.toArray(new TypeSerializerSnapshot<?>[associatedSubclassSerializerSnapshots.size()]));
	}

	/**
	 * Checks whether the new {@link PojoSerializer} has new or removed fields compared to the previous one.
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
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots,
			PojoSerializer<?> newPojoSerializer) {
		Set<Class<?>> previousRegistrationOrder = registeredSubclassSerializerSnapshots.unwrapOptionals().keySet();
		Set<Class<?>> newRegistrationOrder = newPojoSerializer.getRegisteredClasses().keySet();
		return !isPreviousRegistrationPrefixOfNewRegistration(previousRegistrationOrder, newRegistrationOrder);
	}

	private static boolean isPreviousRegistrationPrefixOfNewRegistration(
		Set<Class<?>> previousRegistrationOrder,
		Set<Class<?>> newRegistrationOrder) {
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
	 * Checks whether the previous serializer, represented by this snapshot, has
	 * non-registered subclasses.
	 */
	private static boolean previousSerializerHasNonRegisteredSubclasses(
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots) {
		return nonRegisteredSubclassSerializerSnapshots.size() > 0;
	}

	/**
	 * Creates a reconfigured version of the {@link PojoSerializer}.
	 *
	 * @param originalNewPojoSerializer the original new {@link PojoSerializer} to create a reconfigured version of.
	 * @param fieldSerializerCompatibility compatibility of preexisting fields' serializers.
	 * @param registeredSerializerSnapshots snapshot of previous registered subclasses' serializers.
	 * @param preExistingRegistrationsCompatibility compatibility of preexisting subclasses' serializers.
	 * @param nonRegisteredSubclassSerializerSnapshots snapshot of previous non-registered subclasses' serializers.
	 *
	 * @return a reconfigured version of the original new {@link PojoSerializer}.
	 */
	private static <T> PojoSerializer<T> constructReconfiguredPojoSerializer(
			PojoSerializer<T> originalNewPojoSerializer,
			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> fieldSerializerCompatibility,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSerializerSnapshots,
			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<T> preExistingRegistrationsCompatibility,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots) {

		@SuppressWarnings("unchecked")
		final TypeSerializer<Object>[] reconfiguredFieldSerializers = constructReconfiguredFieldSerializers(fieldSerializerCompatibility);

		Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]> reconfiguredSubclassRegistry = constructReconfiguredSubclassRegistry(
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
			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<?> fieldSerializerCompatibility) {
		checkArgument(!fieldSerializerCompatibility.isIncompatible() && !fieldSerializerCompatibility.isCompatibleAfterMigration());
		return fieldSerializerCompatibility.getNestedSerializers();
	}

	private static Tuple2<LinkedHashMap<Class<?>, Integer>, TypeSerializer<Object>[]> constructReconfiguredSubclassRegistry(
			LinkedHashMap<Class<?>, TypeSerializer<?>> newSubclassRegistrations,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSerializerSnapshots,
			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<?> preExistingRegistrationsCompatibility) {

		checkArgument(!preExistingRegistrationsCompatibility.isIncompatible() && !preExistingRegistrationsCompatibility.isCompatibleAfterMigration());

		LinkedHashMap<Class<?>, TypeSerializer<?>> reconfiguredSubclassSerializerRegistry =
			restoreSerializers(registeredSerializerSnapshots.unwrapOptionals());

		Iterator<TypeSerializer<?>> serializersForPreexistingRegistrations =
			Arrays.asList(preExistingRegistrationsCompatibility.getNestedSerializers()).iterator();

		for (Map.Entry<Class<?>, TypeSerializer<?>> registration : newSubclassRegistrations.entrySet()) {
			// new registrations should simply be appended to the subclass serializer registry with their new serializers;
			// preexisting registrations should use the compatibility-checked serializer
			TypeSerializer<?> newRegistration = (reconfiguredSubclassSerializerRegistry.containsKey(registration.getKey()))
				? serializersForPreexistingRegistrations.next()
				: registration.getValue();
			reconfiguredSubclassSerializerRegistry.put(registration.getKey(), newRegistration);
		}

		return decomposeSubclassSerializerRegistry(reconfiguredSubclassSerializerRegistry);
	}
}
