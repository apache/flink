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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.writeVersionedSnapshot;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TypeSerializerSnapshot} of {@link RowDataKeySerializer}. */
public class RowDataKeySerializerSnapshot implements TypeSerializerSnapshot<RowDataKey> {

    private RowDataKeySerializer serializer;
    private TypeSerializerSnapshot<RowData> restoredRowDataSerializerSnapshot;

    @SuppressWarnings("unused")
    public RowDataKeySerializerSnapshot() {
        // this constructor is used when restoring from a checkpoint/savepoint.
    }

    public RowDataKeySerializerSnapshot(RowDataKeySerializer serializer) {
        this.serializer = checkNotNull(serializer);
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        store(serializer.equaliser, out);
        store(serializer.hashFunction, out);
        writeVersionedSnapshot(out, serializer.serializer.snapshotConfiguration());
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        checkArgument(readVersion == 0, "Unexpected version: " + readVersion);

        GeneratedRecordEqualiser equaliser = restore(in, userCodeClassLoader);
        GeneratedHashFunction hashFunction = restore(in, userCodeClassLoader);

        restoredRowDataSerializerSnapshot =
                TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);

        serializer =
                new RowDataKeySerializer(
                        restoredRowDataSerializerSnapshot.restoreSerializer(),
                        equaliser.newInstance(userCodeClassLoader),
                        hashFunction.newInstance(userCodeClassLoader),
                        equaliser,
                        hashFunction);
    }

    private static void store(Object object, DataOutputView out) throws IOException {
        byte[] bytes = InstantiationUtil.serializeObject(object);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private <T> T restore(DataInputView in, ClassLoader classLoader) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.read(bytes);
        try {
            return InstantiationUtil.deserializeObject(bytes, classLoader); // here
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public TypeSerializer<RowDataKey> restoreSerializer() {
        return serializer;
    }

    @Override
    public TypeSerializerSchemaCompatibility<RowDataKey> resolveSchemaCompatibility(
            TypeSerializerSnapshot<RowDataKey> oldSerializerSnapshot) {
        if (!(oldSerializerSnapshot instanceof RowDataKeySerializerSnapshot)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        RowDataKeySerializerSnapshot old = (RowDataKeySerializerSnapshot) oldSerializerSnapshot;

        TypeSerializerSchemaCompatibility<RowData> compatibility =
                old.restoredRowDataSerializerSnapshot.resolveSchemaCompatibility(
                        old.serializer.serializer.snapshotConfiguration());

        return mapToOuterCompatibility(
                compatibility,
                serializer.equalizerInstance,
                serializer.hashFunctionInstance,
                serializer.equaliser,
                serializer.hashFunction);
    }

    private static TypeSerializerSchemaCompatibility<RowDataKey> mapToOuterCompatibility(
            TypeSerializerSchemaCompatibility<RowData> rowDataCmp,
            RecordEqualiser equaliserInstance,
            HashFunction hashFunctionInstance,
            GeneratedRecordEqualiser equaliser,
            GeneratedHashFunction hashFunction) {
        if (rowDataCmp.isCompatibleAsIs()) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else if (rowDataCmp.isCompatibleAfterMigration()) {
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        } else if (rowDataCmp.isCompatibleWithReconfiguredSerializer()) {
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    new RowDataKeySerializer(
                            rowDataCmp.getReconfiguredSerializer(),
                            equaliserInstance,
                            hashFunctionInstance,
                            equaliser,
                            hashFunction));
        } else if (rowDataCmp.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        } else {
            throw new UnsupportedOperationException("Unknown compatibility mode: " + rowDataCmp);
        }
    }
}
