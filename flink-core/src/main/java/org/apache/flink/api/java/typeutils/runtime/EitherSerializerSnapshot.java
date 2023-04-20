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
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Either;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Configuration snapshot for the {@link EitherSerializer}.
 *
 * @deprecated this snapshot class is no longer used by any serializers. Instead, {@link
 *     JavaEitherSerializerSnapshot} is used.
 */
@Internal
@Deprecated
public final class EitherSerializerSnapshot<L, R> implements TypeSerializerSnapshot<Either<L, R>> {

    private static final int CURRENT_VERSION = 2;

    /** Snapshot handling for the component serializer snapshot. */
    @Nullable private NestedSerializersSnapshotDelegate nestedSnapshot;

    /** Constructor for read instantiation. */
    @SuppressWarnings("unused")
    public EitherSerializerSnapshot() {}

    /** Constructor to create the snapshot for writing. */
    public EitherSerializerSnapshot(
            TypeSerializer<L> leftSerializer, TypeSerializer<R> rightSerializer) {

        this.nestedSnapshot =
                new NestedSerializersSnapshotDelegate(leftSerializer, rightSerializer);
    }

    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        checkState(nestedSnapshot != null);
        nestedSnapshot.writeNestedSerializerSnapshots(out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
            throws IOException {
        switch (readVersion) {
            case 1:
                throw new UnsupportedOperationException(
                        String.format(
                                "No longer supported version [%d]. Please upgrade first to Flink 1.16. ",
                                readVersion));
            case 2:
                readV2(in, classLoader);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized version: " + readVersion);
        }
    }

    private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
        nestedSnapshot =
                NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(in, classLoader);
    }

    @Override
    public EitherSerializer<L, R> restoreSerializer() {
        checkState(nestedSnapshot != null);
        return new EitherSerializer<>(
                nestedSnapshot.getRestoredNestedSerializer(0),
                nestedSnapshot.getRestoredNestedSerializer(1));
    }

    @Override
    public TypeSerializerSchemaCompatibility<Either<L, R>> resolveSchemaCompatibility(
            TypeSerializer<Either<L, R>> newSerializer) {
        checkState(nestedSnapshot != null);

        if (newSerializer instanceof EitherSerializer) {
            // delegate compatibility check to the new snapshot class
            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer,
                    new JavaEitherSerializerSnapshot<>(),
                    nestedSnapshot.getNestedSerializerSnapshots());
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
