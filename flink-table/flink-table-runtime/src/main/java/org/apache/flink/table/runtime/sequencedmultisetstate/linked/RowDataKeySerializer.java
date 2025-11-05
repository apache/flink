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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TypeSerializer} for {@link RowDataKey}. */
@Internal
public class RowDataKeySerializer extends TypeSerializer<RowDataKey> {
    final TypeSerializer<RowData> serializer;
    final GeneratedRecordEqualiser equaliser; // used to snapshot
    final GeneratedHashFunction hashFunction; // used to snapshot
    final RecordEqualiser equalizerInstance; // passed to restored keys
    final HashFunction hashFunctionInstance; // passed to restored keys

    public RowDataKeySerializer(
            TypeSerializer<RowData> serializer,
            RecordEqualiser equalizerInstance,
            HashFunction hashFunctionInstance,
            GeneratedRecordEqualiser equaliser,
            GeneratedHashFunction hashFunction) {
        this.serializer = checkNotNull(serializer);
        this.equalizerInstance = checkNotNull(equalizerInstance);
        this.hashFunctionInstance = checkNotNull(hashFunctionInstance);
        this.equaliser = checkNotNull(equaliser);
        this.hashFunction = checkNotNull(hashFunction);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<RowDataKey> duplicate() {
        return new RowDataKeySerializer(
                serializer.duplicate(),
                equalizerInstance,
                hashFunctionInstance,
                equaliser,
                hashFunction);
    }

    @Override
    public RowDataKey createInstance() {
        return new RowDataKey(equalizerInstance, hashFunctionInstance);
    }

    @Override
    public RowDataKey copy(RowDataKey from) {
        return RowDataKey.toKeyNotProjected(
                serializer.copy(from.rowData), equalizerInstance, hashFunctionInstance);
    }

    @Override
    public RowDataKey copy(RowDataKey from, RowDataKey reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return serializer.getLength();
    }

    @Override
    public void serialize(RowDataKey record, DataOutputView target) throws IOException {
        serializer.serialize(record.rowData, target);
    }

    @Override
    public RowDataKey deserialize(DataInputView source) throws IOException {
        return RowDataKey.toKeyNotProjected(
                serializer.deserialize(source), equalizerInstance, hashFunctionInstance);
    }

    @Override
    public RowDataKey deserialize(RowDataKey reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowDataKeySerializer) {
            RowDataKeySerializer other = (RowDataKeySerializer) obj;
            return serializer.equals(other.serializer)
                    && equalizerInstance.equals(other.equalizerInstance)
                    && hashFunctionInstance.equals(other.hashFunctionInstance);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serializer, equalizerInstance, hashFunctionInstance);
    }

    @Override
    public TypeSerializerSnapshot<RowDataKey> snapshotConfiguration() {
        return new RowDataKeySerializerSnapshot(this);
    }
}
