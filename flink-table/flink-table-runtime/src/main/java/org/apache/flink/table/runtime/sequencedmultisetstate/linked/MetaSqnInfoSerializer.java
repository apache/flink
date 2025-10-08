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
