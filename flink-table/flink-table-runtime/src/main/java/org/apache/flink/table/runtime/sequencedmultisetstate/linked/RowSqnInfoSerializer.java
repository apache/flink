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

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

@SuppressWarnings("ClassEscapesDefinedScope")
public class RowSqnInfoSerializer extends CompositeSerializer<RowSqnInfo> {

    public RowSqnInfoSerializer() {
        this(null, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
    }

    protected RowSqnInfoSerializer(
            PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
        super(
                PrecomputedParameters.precompute(
                        true, true, (TypeSerializer<Object>[]) fieldSerializers),
                fieldSerializers);
    }

    @Override
    public RowSqnInfo createInstance(Object... values) {
        return new RowSqnInfo((Long) values[0], (Long) values[1]);
    }

    @Override
    protected void setField(RowSqnInfo sqnInfo, int index, Object fieldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getField(RowSqnInfo value, int index) {
        switch (index) {
            case 0:
                return value.firstSqn;
            case 1:
                return value.lastSqn;
            default:
                throw new IllegalArgumentException("invalid index: " + index);
        }
    }

    @Override
    protected CompositeSerializer<RowSqnInfo> createSerializerInstance(
            PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
        return new RowSqnInfoSerializer(precomputed, originalSerializers);
    }

    @Override
    public TypeSerializerSnapshot<RowSqnInfo> snapshotConfiguration() {
        return new RowSqnInfoSerializerSnapshot(this);
    }

    public static class RowSqnInfoSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<RowSqnInfo, RowSqnInfoSerializer> {

        @SuppressWarnings("unused")
        public RowSqnInfoSerializerSnapshot() {}

        RowSqnInfoSerializerSnapshot(RowSqnInfoSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return 0;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(RowSqnInfoSerializer outerSerializer) {
            return new TypeSerializer[] {LongSerializer.INSTANCE, LongSerializer.INSTANCE};
        }

        @Override
        protected RowSqnInfoSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new RowSqnInfoSerializer(null, nestedSerializers);
        }
    }
}
