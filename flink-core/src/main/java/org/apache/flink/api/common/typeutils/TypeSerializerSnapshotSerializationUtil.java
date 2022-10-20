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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Optional;

/** Utility methods for serialization of {@link TypeSerializerSnapshot}. */
public class TypeSerializerSnapshotSerializationUtil {

    /**
     * Writes a {@link TypeSerializerSnapshot} to the provided data output view.
     *
     * <p>It is written with a format that can be later read again using {@link
     * #readSerializerSnapshot(DataInputView, ClassLoader)}.
     *
     * @param out the data output view
     * @param serializerSnapshot the serializer configuration snapshot to write
     */
    public static <T> void writeSerializerSnapshot(
            DataOutputView out, TypeSerializerSnapshot<T> serializerSnapshot) throws IOException {

        new TypeSerializerSnapshotSerializationProxy<>(serializerSnapshot).write(out);
    }

    /**
     * Reads from a data input view a {@link TypeSerializerSnapshot} that was previously written
     * using {@link #writeSerializerSnapshot(DataOutputView, TypeSerializerSnapshot}.
     *
     * @param in the data input view
     * @param userCodeClassLoader the user code class loader to use
     * @return the read serializer configuration snapshot
     */
    public static <T> TypeSerializerSnapshot<T> readSerializerSnapshot(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

        final TypeSerializerSnapshotSerializationProxy<T> proxy =
                new TypeSerializerSnapshotSerializationProxy<>(userCodeClassLoader);
        proxy.read(in);

        return proxy.getSerializerSnapshot();
    }

    public static <T> TypeSerializerSnapshot<T> readAndInstantiateSnapshotClass(
            DataInputView in, ClassLoader cl) throws IOException {
        Class<TypeSerializerSnapshot<T>> clazz =
                InstantiationUtil.resolveClassByName(in, cl, TypeSerializerSnapshot.class);

        return InstantiationUtil.instantiate(clazz);
    }

    /** Utility serialization proxy for a {@link TypeSerializerSnapshot}. */
    static final class TypeSerializerSnapshotSerializationProxy<T>
            extends VersionedIOReadableWritable {

        private static final int VERSION = 2;

        private ClassLoader userCodeClassLoader;
        private TypeSerializerSnapshot<T> serializerSnapshot;

        /** Constructor for reading serializers. */
        TypeSerializerSnapshotSerializationProxy(ClassLoader userCodeClassLoader) {
            this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
        }

        /** Constructor for writing out serializers. */
        TypeSerializerSnapshotSerializationProxy(
                TypeSerializerSnapshot<T> serializerConfigSnapshot) {
            this.serializerSnapshot = Preconditions.checkNotNull(serializerConfigSnapshot);
        }

        /**
         * Binary format layout of a written serializer snapshot is as follows:
         *
         * <ul>
         *   <li>1. Format version of this util.
         *   <li>2. Name of the TypeSerializerSnapshot class.
         *   <li>3. The version of the TypeSerializerSnapshot's binary format.
         *   <li>4. The actual serializer snapshot data.
         * </ul>
         */
        @SuppressWarnings("deprecation")
        @Override
        public void write(DataOutputView out) throws IOException {
            // write the format version of this utils format
            super.write(out);

            TypeSerializerSnapshot.writeVersionedSnapshot(out, serializerSnapshot);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(DataInputView in) throws IOException {
            // read version
            super.read(in);
            final int version = getReadVersion();

            switch (version) {
                case 2:
                    serializerSnapshot = deserializeV2(in, userCodeClassLoader);
                    break;
                default:
                    throw new IOException(
                            "Unrecognized version for TypeSerializerSnapshot format: " + version);
            }
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public int[] getCompatibleVersions() {
            return new int[] {VERSION};
        }

        @Override
        public Optional<String> getAdditionalDetailsForIncompatibleVersion(int readVersion) {
            if (readVersion == 1) {
                return Optional.of(
                        "As of Flink 1.17 TypeSerializerConfigSnapshot is no longer supported. "
                                + "In order to upgrade Flink to 1.17+ you need to first migrate your "
                                + "serializers to use TypeSerializerSnapshot instead. Please first take a "
                                + "savepoint in Flink 1.16 without using TypeSerializerConfigSnapshot. "
                                + "After that you can use this savepoint to upgrade to Flink 1.17.");
            }
            return Optional.empty();
        }

        TypeSerializerSnapshot<T> getSerializerSnapshot() {
            return serializerSnapshot;
        }

        /** Deserialization path for Flink versions 1.7+. */
        @VisibleForTesting
        static <T> TypeSerializerSnapshot<T> deserializeV2(DataInputView in, ClassLoader cl)
                throws IOException {
            return TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
        }
    }
}
