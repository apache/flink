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
public class MetaSqnInfoSerializer extends CompositeSerializer<MetaSqnInfo> {

    public MetaSqnInfoSerializer() {
        this(null, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
    }

    protected MetaSqnInfoSerializer(
            PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
        super(
                PrecomputedParameters.precompute(
                        true, true, (TypeSerializer<Object>[]) fieldSerializers),
                fieldSerializers);
    }

    @Override
    public MetaSqnInfo createInstance(Object... values) {
        return new MetaSqnInfo((Long) values[0], (Long) values[1]);
    }

    @Override
    protected void setField(MetaSqnInfo sqnInfo, int index, Object fieldValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getField(MetaSqnInfo value, int index) {
        switch (index) {
            case 0:
                return value.highSqn;
            case 1:
                return value.size;
            default:
                throw new IllegalArgumentException("invalid index: " + index);
        }
    }

    @Override
    protected CompositeSerializer<MetaSqnInfo> createSerializerInstance(
            PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
        return new MetaSqnInfoSerializer(precomputed, originalSerializers);
    }

    @Override
    public TypeSerializerSnapshot<MetaSqnInfo> snapshotConfiguration() {
        return new MetaSqnInfoSerializerSnapshot(this);
    }

    public static class MetaSqnInfoSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<MetaSqnInfo, MetaSqnInfoSerializer> {

        @SuppressWarnings("unused")
        public MetaSqnInfoSerializerSnapshot() {}

        MetaSqnInfoSerializerSnapshot(MetaSqnInfoSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return 0;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(MetaSqnInfoSerializer outerSerializer) {
            return new TypeSerializer[] {LongSerializer.INSTANCE, LongSerializer.INSTANCE};
        }

        @Override
        protected MetaSqnInfoSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new MetaSqnInfoSerializer(null, nestedSerializers);
        }
    }
}
