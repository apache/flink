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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;

/**
 * Utility methods for serialization of {@link TypeSerializer}.
 *
 * @deprecated This utility class was used to write serializers into checkpoints. Starting from
 *     Flink 1.6.x, this should no longer happen, and therefore this class is deprecated. It remains
 *     here for backwards compatibility paths.
 */
@Internal
@Deprecated
public class TypeSerializerSerializationUtil {

    private static final Logger LOG =
            LoggerFactory.getLogger(TypeSerializerSerializationUtil.class);

    /**
     * Writes a {@link TypeSerializer} to the provided data output view.
     *
     * <p>It is written with a format that can be later read again using {@link
     * #tryReadSerializer(DataInputView, ClassLoader, boolean)}.
     *
     * @param out the data output view.
     * @param serializer the serializer to write.
     * @param <T> Data type of the serializer.
     * @throws IOException
     */
    public static <T> void writeSerializer(DataOutputView out, TypeSerializer<T> serializer)
            throws IOException {
        new TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<>(serializer)
                .write(out);
    }

    /**
     * Reads from a data input view a {@link TypeSerializer} that was previously written using
     * {@link #writeSerializer(DataOutputView, TypeSerializer)}.
     *
     * <p>If deserialization fails for any reason (corrupted serializer bytes, serializer class no
     * longer in classpath, serializer class no longer valid, etc.), an {@link IOException} is
     * thrown.
     *
     * @param in the data input view.
     * @param userCodeClassLoader the user code class loader to use.
     * @param <T> Data type of the serializer.
     * @return the deserialized serializer.
     */
    public static <T> TypeSerializer<T> tryReadSerializer(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        return tryReadSerializer(in, userCodeClassLoader, false);
    }

    /**
     * Reads from a data input view a {@link TypeSerializer} that was previously written using
     * {@link #writeSerializer(DataOutputView, TypeSerializer)}.
     *
     * <p>If deserialization fails due to any exception, users can opt to use a dummy {@link
     * UnloadableDummyTypeSerializer} to hold the serializer bytes, otherwise an {@link IOException}
     * is thrown.
     *
     * @param in the data input view.
     * @param userCodeClassLoader the user code class loader to use.
     * @param useDummyPlaceholder whether or not to use a dummy {@link
     *     UnloadableDummyTypeSerializer} to hold the serializer bytes in the case of a {@link
     *     ClassNotFoundException} or {@link InvalidClassException}.
     * @param <T> Data type of the serializer.
     * @return the deserialized serializer.
     */
    public static <T> TypeSerializer<T> tryReadSerializer(
            DataInputView in, ClassLoader userCodeClassLoader, boolean useDummyPlaceholder)
            throws IOException {

        final TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<T> proxy =
                new TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<>(
                        userCodeClassLoader);

        try {
            proxy.read(in);
            return proxy.getTypeSerializer();
        } catch (UnloadableTypeSerializerException e) {
            if (useDummyPlaceholder) {
                LOG.warn(
                        "Could not read a requested serializer. Replaced with a UnloadableDummyTypeSerializer.",
                        e.getCause());
                return new UnloadableDummyTypeSerializer<>(e.getSerializerBytes(), e.getCause());
            } else {
                throw e;
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------

    /** Utility serialization proxy for a {@link TypeSerializer}. */
    public static final class TypeSerializerSerializationProxy<T>
            extends VersionedIOReadableWritable {

        private static final int VERSION = 1;

        private ClassLoader userClassLoader;
        private TypeSerializer<T> typeSerializer;

        public TypeSerializerSerializationProxy(ClassLoader userClassLoader) {
            this.userClassLoader = userClassLoader;
        }

        public TypeSerializerSerializationProxy(TypeSerializer<T> typeSerializer) {
            this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
        }

        public TypeSerializer<T> getTypeSerializer() {
            return typeSerializer;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);

            if (typeSerializer instanceof UnloadableDummyTypeSerializer) {
                UnloadableDummyTypeSerializer<T> dummyTypeSerializer =
                        (UnloadableDummyTypeSerializer<T>) this.typeSerializer;

                byte[] serializerBytes = dummyTypeSerializer.getActualBytes();
                out.write(serializerBytes.length);
                out.write(serializerBytes);
            } else {
                // write in a way that allows the stream to recover from exceptions
                try (ByteArrayOutputStreamWithPos streamWithPos =
                        new ByteArrayOutputStreamWithPos()) {
                    InstantiationUtil.serializeObject(streamWithPos, typeSerializer);
                    out.writeInt(streamWithPos.getPosition());
                    out.write(streamWithPos.getBuf(), 0, streamWithPos.getPosition());
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(DataInputView in) throws IOException {
            super.read(in);

            // read in a way that allows the stream to recover from exceptions
            int serializerBytes = in.readInt();
            byte[] buffer = new byte[serializerBytes];
            in.readFully(buffer);

            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            try (InstantiationUtil.FailureTolerantObjectInputStream ois =
                    new InstantiationUtil.FailureTolerantObjectInputStream(
                            new ByteArrayInputStream(buffer), userClassLoader)) {

                Thread.currentThread().setContextClassLoader(userClassLoader);
                typeSerializer = (TypeSerializer<T>) ois.readObject();
            } catch (Exception e) {
                throw new UnloadableTypeSerializerException(e, buffer);
            } finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
            }
        }

        @Override
        public int getVersion() {
            return VERSION;
        }
    }

    // ------------------------------------------------------------------------
    //  utility exception
    // ------------------------------------------------------------------------

    /**
     * An exception thrown to indicate that a serializer cannot be read. It wraps the cause of the
     * read error, as well as the original bytes of the written serializer.
     */
    @Internal
    private static class UnloadableTypeSerializerException extends IOException {

        private static final long serialVersionUID = 1L;

        private final byte[] serializerBytes;

        /**
         * Creates a new exception, with the cause of the read error and the original serializer
         * bytes.
         *
         * @param cause the cause of the read error.
         * @param serializerBytes the original serializer bytes.
         */
        public UnloadableTypeSerializerException(Exception cause, byte[] serializerBytes) {
            super(cause);
            this.serializerBytes = Preconditions.checkNotNull(serializerBytes);
        }

        public byte[] getSerializerBytes() {
            return serializerBytes;
        }
    }
}
