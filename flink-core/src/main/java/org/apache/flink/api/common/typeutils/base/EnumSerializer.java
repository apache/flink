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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link TypeSerializer} for Java enums. */
@Internal
public final class EnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> enumClass;

    /**
     * Maintain our own map of enum value to their ordinal, instead of directly using {@link
     * Enum#ordinal()}. This allows us to maintain backwards compatibility for previous serialized
     * data in the case that the order of enum constants was changed or new constants were added.
     *
     * <p>On a fresh start with no reconfiguration, the ordinals would simply be identical to the
     * enum constants actual ordinals. Ordinals may change after reconfiguration.
     */
    private Map<T, Integer> valueToOrdinal;

    /**
     * Array of enum constants with their indexes identical to their ordinals in the {@link
     * #valueToOrdinal} map. Serves as a bidirectional map to have fast access from ordinal to
     * value. May be reordered after reconfiguration.
     */
    private T[] values;

    public EnumSerializer(Class<T> enumClass) {
        this(enumClass, enumClass.getEnumConstants());
    }

    private EnumSerializer(Class<T> enumClass, T[] enumValues) {
        this.enumClass = checkNotNull(enumClass);
        this.values = checkNotNull(enumValues);
        checkArgument(Enum.class.isAssignableFrom(enumClass), "not an enum");

        checkArgument(this.values.length > 0, "cannot use an empty enum");

        this.valueToOrdinal = new EnumMap<>(this.enumClass);
        int i = 0;
        for (T value : values) {
            this.valueToOrdinal.put(value, i++);
        }
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public EnumSerializer<T> duplicate() {
        return this;
    }

    @Override
    public T createInstance() {
        checkState(values != null);
        return values[0];
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        // use our own maintained ordinals instead of the actual enum ordinal
        target.writeInt(valueToOrdinal.get(record));
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return values[source.readInt()];
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return values[source.readInt()];
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 4);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EnumSerializer) {
            EnumSerializer<?> other = (EnumSerializer<?>) obj;

            return other.enumClass == this.enumClass && Arrays.equals(values, other.values);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return enumClass.hashCode();
    }

    // --------------------------------------------------------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // may be null if this serializer was deserialized from an older version
        if (this.values == null) {
            this.values = enumClass.getEnumConstants();

            this.valueToOrdinal = new EnumMap<>(this.enumClass);
            int i = 0;
            for (T value : values) {
                this.valueToOrdinal.put(value, i++);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public EnumSerializerSnapshot<T> snapshotConfiguration() {
        return new EnumSerializerSnapshot<>(enumClass, values);
    }

    /** {@link TypeSerializerSnapshot} for {@link EnumSerializer}. */
    public static final class EnumSerializerSnapshot<T extends Enum<T>>
            implements TypeSerializerSnapshot<T> {
        private static final int CURRENT_VERSION = 3;

        private T[] previousEnums;
        private Class<T> enumClass;

        @SuppressWarnings("unused")
        public EnumSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        EnumSerializerSnapshot(Class<T> enumClass, T[] enums) {
            this.enumClass = checkNotNull(enumClass);
            this.previousEnums = checkNotNull(enums);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            checkState(enumClass != null, "Enum class can not be null.");
            out.writeUTF(enumClass.getName());
            out.writeInt(previousEnums.length);
            for (T enumConstant : previousEnums) {
                out.writeUTF(enumConstant.name());
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            enumClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);

            int numEnumConstants = in.readInt();

            @SuppressWarnings("unchecked")
            T[] previousEnums = (T[]) Array.newInstance(enumClass, numEnumConstants);
            for (int i = 0; i < numEnumConstants; i++) {
                String enumName = in.readUTF();
                try {
                    previousEnums[i] = Enum.valueOf(enumClass, enumName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException(
                            "Could not create a restore serializer for enum "
                                    + enumClass
                                    + ". Probably because an enum value was removed.");
                }
            }

            this.previousEnums = previousEnums;
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            checkState(enumClass != null, "Enum class can not be null.");

            return new EnumSerializer<>(enumClass, previousEnums);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof EnumSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            EnumSerializer<T> newEnumSerializer = (EnumSerializer<T>) newSerializer;
            if (!enumClass.equals(newEnumSerializer.enumClass)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            T[] currentEnums = enumClass.getEnumConstants();

            if (Arrays.equals(previousEnums, currentEnums)) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }

            Set<T> reconfiguredEnumSet = new LinkedHashSet<>(Arrays.asList(previousEnums));
            reconfiguredEnumSet.addAll(Arrays.asList(currentEnums));

            @SuppressWarnings("unchecked")
            T[] reconfiguredEnums =
                    reconfiguredEnumSet.toArray(
                            (T[]) Array.newInstance(enumClass, reconfiguredEnumSet.size()));

            EnumSerializer<T> reconfiguredSerializer =
                    new EnumSerializer<>(enumClass, reconfiguredEnums);
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    reconfiguredSerializer);
        }
    }

    /**
     * Configuration snapshot of a serializer for enumerations.
     *
     * <p>Configuration contains the enum class, and an array of the enum's constants that existed
     * when the configuration snapshot was taken.
     *
     * @param <T> the enum type.
     */
    @Deprecated
    public static final class EnumSerializerConfigSnapshot<T extends Enum<T>>
            extends GenericTypeSerializerConfigSnapshot<T> {

        private static final int VERSION = 2;

        private List<String> enumConstants;

        /** This empty nullary constructor is required for deserializing the configuration. */
        @SuppressWarnings("unused")
        public EnumSerializerConfigSnapshot() {}

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);

            out.writeInt(enumConstants.size());
            for (String enumConstant : enumConstants) {
                out.writeUTF(enumConstant);
            }
        }

        @Override
        public void read(DataInputView in) throws IOException {
            super.read(in);

            if (getReadVersion() == 1) {
                try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
                    try {
                        T[] legacyEnumConstants =
                                InstantiationUtil.deserializeObject(
                                        inViewWrapper, getUserCodeClassLoader());
                        this.enumConstants = buildEnumConstantsList(legacyEnumConstants);
                    } catch (ClassNotFoundException e) {
                        throw new IOException(
                                "The requested enum class cannot be found in classpath.", e);
                    } catch (IllegalArgumentException e) {
                        throw new IOException(
                                "A previously existing enum constant of "
                                        + getTypeClass().getName()
                                        + " no longer exists.",
                                e);
                    }
                }
            } else if (getReadVersion() == VERSION) {
                int numEnumConstants = in.readInt();

                this.enumConstants = new ArrayList<>(numEnumConstants);
                for (int i = 0; i < numEnumConstants; i++) {
                    enumConstants.add(in.readUTF());
                }
            } else {
                throw new IOException(
                        "Cannot deserialize EnumSerializerConfigSnapshot with version "
                                + getReadVersion());
            }
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {

            Class<T> enumClass = getTypeClass();

            @SuppressWarnings("unchecked")
            T[] previousEnums = (T[]) Array.newInstance(enumClass, enumConstants.size());

            for (int i = 0; i < enumConstants.size(); i++) {
                String enumName = enumConstants.get(i);
                try {
                    previousEnums[i] = Enum.valueOf(enumClass, enumName);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(
                            "Could not create a restore serializer for enum "
                                    + enumClass
                                    + ". Probably because an enum value was removed.");
                }
            }
            return new EnumSerializerSnapshot<>(enumClass, previousEnums)
                    .resolveSchemaCompatibility(newSerializer);
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public int[] getCompatibleVersions() {
            return new int[] {VERSION, 1};
        }

        List<String> getEnumConstants() {
            return enumConstants;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj)
                    && enumConstants.equals(
                            ((EnumSerializerConfigSnapshot) obj).getEnumConstants());
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 31 + enumConstants.hashCode();
        }

        private static <T extends Enum<T>> List<String> buildEnumConstantsList(
                T[] enumConstantsArr) {
            List<String> res = new ArrayList<>(enumConstantsArr.length);

            for (T enumConstant : enumConstantsArr) {
                res.add(enumConstant.name());
            }

            return res;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    T[] getValues() {
        return values;
    }

    @VisibleForTesting
    Map<T, Integer> getValueToOrdinal() {
        return valueToOrdinal;
    }

    @Override
    public String toString() {
        return "EnumSerializer{"
                + "enumClass="
                + enumClass
                + ", values="
                + Arrays.toString(values)
                + '}';
    }
}
