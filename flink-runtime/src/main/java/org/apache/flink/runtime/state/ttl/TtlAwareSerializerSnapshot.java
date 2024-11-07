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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link TypeSerializerSnapshot} for TtlAwareSerializer. This class wraps a {@link
 * TypeSerializerSnapshot} with ttl awareness. It will return true when the wrapped {@link
 * TypeSerializerSnapshot} is the instance of {@link TtlStateFactory.TtlSerializerSnapshot}. Also,
 * it overrides the compatibility type check between TtlSerializerSnapshot and non-ttl
 * TypeSerializerSnapshot.
 *
 * <p>If two TtlAwareSerializerSnapshots have the same ttl config, it will return the compatibility
 * check result of the original TypeSerializerSnapshot.
 *
 * <p>If two TtlAwareSerializerSnapshots have different ttl config, it will return a wrapped
 * compatibility check result.
 */
public class TtlAwareSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private static final int CURRENT_VERSION = 1;

    private boolean isTtlEnabled;

    private TypeSerializerSnapshot<T> typeSerializerSnapshot;

    @SuppressWarnings({"WeakerAccess", "unused"})
    public TtlAwareSerializerSnapshot() {}

    public TtlAwareSerializerSnapshot(
            TypeSerializerSnapshot<T> typeSerializerSnapshot, boolean isTtlEnabled) {
        this.typeSerializerSnapshot = typeSerializerSnapshot;
        this.isTtlEnabled = isTtlEnabled;
    }

    public TtlAwareSerializerSnapshot(TypeSerializerSnapshot<T> typeSerializerSnapshot) {
        this.typeSerializerSnapshot = typeSerializerSnapshot;
        this.isTtlEnabled = typeSerializerSnapshot instanceof TtlStateFactory.TtlSerializerSnapshot;
    }

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeBoolean(isTtlEnabled);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, typeSerializerSnapshot);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        this.isTtlEnabled = in.readBoolean();
        this.typeSerializerSnapshot =
                TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new TtlAwareSerializer<>(typeSerializerSnapshot.restoreSerializer());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSerializerSnapshot) {
        if (!(oldSerializerSnapshot instanceof TtlAwareSerializerSnapshot)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        TtlAwareSerializerSnapshot<T> oldTtlAwareSerializerSnapshot =
                (TtlAwareSerializerSnapshot<T>) oldSerializerSnapshot;
        if (!oldTtlAwareSerializerSnapshot.isTtlEnabled() && this.isTtlEnabled()) {
            // Check compatibility from disabling to enabling ttl
            TtlStateFactory.TtlSerializerSnapshot<T> newSerializerSnapshot =
                    (TtlStateFactory.TtlSerializerSnapshot<T>)
                            this.getOrinalTypeSerializerSnapshot();
            TypeSerializerSchemaCompatibility<T> compatibility =
                    newSerializerSnapshot
                            .getValueSerializerSnapshot()
                            .resolveSchemaCompatibility(
                                    oldTtlAwareSerializerSnapshot
                                            .getOrinalTypeSerializerSnapshot());

            return resolveCompatibilityForTtlMigration(compatibility);
        } else if (oldTtlAwareSerializerSnapshot.isTtlEnabled() && !this.isTtlEnabled()) {
            // Check compatibility from enabling to disabling ttl
            TtlStateFactory.TtlSerializerSnapshot<T> oldTtlSerializerSnapshot =
                    (TtlStateFactory.TtlSerializerSnapshot<T>)
                            oldTtlAwareSerializerSnapshot.getOrinalTypeSerializerSnapshot();
            TypeSerializerSchemaCompatibility<T> compatibility =
                    this.getOrinalTypeSerializerSnapshot()
                            .resolveSchemaCompatibility(
                                    oldTtlSerializerSnapshot.getValueSerializerSnapshot());

            return resolveCompatibilityForTtlMigration(compatibility);
        }

        // Check compatibility with the same ttl config
        return this.getOrinalTypeSerializerSnapshot()
                .resolveSchemaCompatibility(
                        oldTtlAwareSerializerSnapshot.getOrinalTypeSerializerSnapshot());
    }

    public boolean isTtlEnabled() {
        return isTtlEnabled;
    }

    public TypeSerializerSnapshot<T> getOrinalTypeSerializerSnapshot() {
        return typeSerializerSnapshot;
    }

    private TypeSerializerSchemaCompatibility<T> resolveCompatibilityForTtlMigration(
            TypeSerializerSchemaCompatibility<T> originalCompatibility) {
        if (originalCompatibility.isCompatibleAsIs()
                || originalCompatibility.isCompatibleAfterMigration()
                || originalCompatibility.isCompatibleWithReconfiguredSerializer()) {
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
