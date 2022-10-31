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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Serializer wrapper to add support of {@code null} value serialization.
 *
 * <p>If the target serializer does not support {@code null} values of its type, you can use this
 * class to wrap this serializer. This is a generic treatment of {@code null} value serialization
 * which comes with the cost of additional byte in the final serialized value. The {@code
 * NullableSerializer} will intercept {@code null} value serialization case and prepend the target
 * serialized value with a boolean flag marking whether it is {@code null} or not.
 *
 * <pre>{@code
 * TypeSerializer<T> originalSerializer = ...;
 * TypeSerializer<T> serializerWithNullValueSupport = NullableSerializer.wrap(originalSerializer);
 * // or
 * TypeSerializer<T> serializerWithNullValueSupport = NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);
 * }
 * }</pre>
 *
 * @param <T> type to serialize
 */
public class NullableSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 3335569358214720033L;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    @Nonnull private final TypeSerializer<T> originalSerializer;
    private final byte[] padding;

    private NullableSerializer(
            @Nonnull TypeSerializer<T> originalSerializer, boolean padNullValueIfFixedLen) {
        this(
                originalSerializer,
                createPadding(originalSerializer.getLength(), padNullValueIfFixedLen));
    }

    private NullableSerializer(@Nonnull TypeSerializer<T> originalSerializer, byte[] padding) {
        this.originalSerializer = originalSerializer;
        this.padding = padding;
    }

    private static byte[] createPadding(
            int originalSerializerLength, boolean padNullValueIfFixedLen) {
        boolean padNullValue = originalSerializerLength > 0 && padNullValueIfFixedLen;
        return padNullValue ? new byte[originalSerializerLength] : EMPTY_BYTE_ARRAY;
    }

    /**
     * This method tries to serialize {@code null} value with the {@code originalSerializer} and
     * wraps it in case of {@link NullPointerException}, otherwise it returns the {@code
     * originalSerializer}.
     *
     * @param originalSerializer serializer to wrap and add {@code null} support
     * @param padNullValueIfFixedLen pad null value to preserve the fixed length of original
     *     serializer
     * @return serializer which supports {@code null} values
     */
    public static <T> TypeSerializer<T> wrapIfNullIsNotSupported(
            @Nonnull TypeSerializer<T> originalSerializer, boolean padNullValueIfFixedLen) {
        return checkIfNullSupported(originalSerializer)
                ? originalSerializer
                : wrap(originalSerializer, padNullValueIfFixedLen);
    }

    /**
     * This method checks if {@code serializer} supports {@code null} value.
     *
     * @param serializer serializer to check
     */
    public static <T> boolean checkIfNullSupported(@Nonnull TypeSerializer<T> serializer) {
        int length = serializer.getLength() > 0 ? serializer.getLength() : 1;
        DataOutputSerializer dos = new DataOutputSerializer(length);
        try {
            serializer.serialize(null, dos);
        } catch (IOException | RuntimeException e) {
            return false;
        }
        checkArgument(
                serializer.getLength() < 0
                        || serializer.getLength() == dos.getCopyOfBuffer().length,
                "The serialized form of the null value should have the same length "
                        + "as any other if the length is fixed in the serializer");
        DataInputDeserializer dis = new DataInputDeserializer(dos.getSharedBuffer());
        try {
            checkArgument(serializer.deserialize(dis) == null);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unexpected failure to deserialize just serialized null value with %s",
                            serializer.getClass().getName()),
                    e);
        }
        checkArgument(
                serializer.copy(null) == null,
                "Serializer %s has to be able properly copy null value if it can serialize it",
                serializer.getClass().getName());
        return true;
    }

    private boolean padNullValue() {
        return padding.length > 0;
    }

    private int nullPaddingLength() {
        return padding.length;
    }

    private TypeSerializer<T> originalSerializer() {
        return originalSerializer;
    }

    /**
     * This method wraps the {@code originalSerializer} with the {@code NullableSerializer} if not
     * already wrapped.
     *
     * @param originalSerializer serializer to wrap and add {@code null} support
     * @param padNullValueIfFixedLen pad null value to preserve the fixed length of original
     *     serializer
     * @return wrapped serializer which supports {@code null} values
     */
    public static <T> TypeSerializer<T> wrap(
            @Nonnull TypeSerializer<T> originalSerializer, boolean padNullValueIfFixedLen) {
        return originalSerializer instanceof NullableSerializer
                ? originalSerializer
                : new NullableSerializer<>(originalSerializer, padNullValueIfFixedLen);
    }

    @Override
    public boolean isImmutableType() {
        return originalSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        TypeSerializer<T> duplicateOriginalSerializer = originalSerializer.duplicate();
        return duplicateOriginalSerializer == originalSerializer
                ? this
                : new NullableSerializer<>(originalSerializer.duplicate(), padNullValue());
    }

    @Override
    public T createInstance() {
        return originalSerializer.createInstance();
    }

    @Override
    public T copy(T from) {
        return from == null ? null : originalSerializer.copy(from);
    }

    @Override
    public T copy(T from, T reuse) {
        return from == null
                ? null
                : (reuse == null
                        ? originalSerializer.copy(from)
                        : originalSerializer.copy(from, reuse));
    }

    @Override
    public int getLength() {
        return padNullValue() ? 1 + padding.length : -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
            target.write(padding);
        } else {
            target.writeBoolean(false);
            originalSerializer.serialize(record, target);
        }
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        boolean isNull = deserializeNull(source);
        return isNull ? null : originalSerializer.deserialize(source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        boolean isNull = deserializeNull(source);
        return isNull
                ? null
                : (reuse == null
                        ? originalSerializer.deserialize(source)
                        : originalSerializer.deserialize(reuse, source));
    }

    private boolean deserializeNull(DataInputView source) throws IOException {
        boolean isNull = source.readBoolean();
        if (isNull) {
            source.skipBytesToRead(padding.length);
        }
        return isNull;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean isNull = deserializeNull(source);
        target.writeBoolean(isNull);
        if (isNull) {
            target.write(padding);
        } else {
            originalSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && originalSerializer.equals(
                                ((NullableSerializer) obj).originalSerializer));
    }

    @Override
    public int hashCode() {
        return originalSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new NullableSerializerSnapshot<>(this);
    }

    /**
     * Snapshot for serializers of nullable types, containing the snapshot of its original
     * serializer.
     */
    @SuppressWarnings({"unchecked", "WeakerAccess"})
    public static class NullableSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<T, NullableSerializer<T>> {

        private static final int VERSION = 2;
        private int nullPaddingLength;

        @SuppressWarnings("unused")
        public NullableSerializerSnapshot() {
            super(NullableSerializer.class);
        }

        public NullableSerializerSnapshot(NullableSerializer<T> serializerInstance) {
            super(serializerInstance);
            this.nullPaddingLength = serializerInstance.nullPaddingLength();
        }

        private NullableSerializerSnapshot(int nullPaddingLength) {
            super(NullableSerializer.class);
            checkArgument(
                    nullPaddingLength >= 0,
                    "Computed NULL padding can not be negative. %s",
                    nullPaddingLength);

            this.nullPaddingLength = nullPaddingLength;
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(NullableSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.originalSerializer()};
        }

        @Override
        protected NullableSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            checkState(
                    nullPaddingLength >= 0,
                    "Negative padding size after serializer construction: %s",
                    nullPaddingLength);

            final byte[] padding =
                    (nullPaddingLength == 0) ? EMPTY_BYTE_ARRAY : new byte[nullPaddingLength];
            TypeSerializer<T> nestedSerializer = (TypeSerializer<T>) nestedSerializers[0];
            return new NullableSerializer<>(nestedSerializer, padding);
        }

        @Override
        protected void writeOuterSnapshot(DataOutputView out) throws IOException {
            out.writeInt(nullPaddingLength);
        }

        @Override
        protected void readOuterSnapshot(
                int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            nullPaddingLength = in.readInt();
        }

        @Override
        protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(
                NullableSerializer<T> newSerializer) {
            return (nullPaddingLength == newSerializer.nullPaddingLength())
                    ? OuterSchemaCompatibility.COMPATIBLE_AS_IS
                    : OuterSchemaCompatibility.INCOMPATIBLE;
        }
    }
}
