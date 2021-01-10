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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Row;

/** Snapshot class for {@link CRowSerializer}. */
public class CRowSerializerSnapshot extends CompositeTypeSerializerSnapshot<CRow, CRowSerializer> {

    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    public CRowSerializerSnapshot() {
        super(CRowSerializer.class);
    }

    /** Constructor to create the snapshot for writing. */
    public CRowSerializerSnapshot(CRowSerializer serializerInstance) {
        super(serializerInstance);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(CRowSerializer outerSerializer) {
        return new TypeSerializer[] {outerSerializer.rowSerializer()};
    }

    @Override
    protected CRowSerializer createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {

        @SuppressWarnings("unchecked")
        TypeSerializer<Row> rowSerializer = (TypeSerializer<Row>) nestedSerializers[0];

        return new CRowSerializer(rowSerializer);
    }
}
