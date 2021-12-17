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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple base class for TypeSerializerSnapshots, for serializers that have no parameters. The
 * serializer is defined solely by its class name.
 *
 * <p>Serializers that produce these snapshots must be public, have public a zero-argument
 * constructor and cannot be a non-static inner classes.
 */
@PublicEvolving
public abstract class SimpleTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    /**
     * This snapshot starts from version 2 (since Flink 1.7.x), so that version 1 is reserved for
     * implementing backwards compatible code paths in case we decide to make this snapshot
     * backwards compatible with the {@link ParameterlessTypeSerializerConfig}.
     */
    private static final int CURRENT_VERSION = 3;

    /**
     * The class of the serializer for this snapshot. The field is null if the serializer was
     * created for read and has not been read, yet.
     */
    @Nonnull private Supplier<? extends TypeSerializer<T>> serializerSupplier;

    /** Constructor to create snapshot from serializer (writing the snapshot). */
    public SimpleTypeSerializerSnapshot(
            @Nonnull Supplier<? extends TypeSerializer<T>> serializerSupplier) {
        this.serializerSupplier = checkNotNull(serializerSupplier);
    }

    // ------------------------------------------------------------------------
    //  Serializer Snapshot Methods
    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return serializerSupplier.get();
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {

        return newSerializer.getClass() == serializerSupplier.get().getClass()
                ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                : TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        //
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
            throws IOException {
        switch (readVersion) {
            case 3:
                {
                    break;
                }
            case 2:
                {
                    // we don't need the classname any more; read and drop to maintain compatibility
                    in.readUTF();
                    break;
                }
            default:
                {
                    throw new IOException("Unrecognized version: " + readVersion);
                }
        }
    }

    // ------------------------------------------------------------------------
    //  standard utilities
    // ------------------------------------------------------------------------

    @Override
    public final boolean equals(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getName();
    }
}
