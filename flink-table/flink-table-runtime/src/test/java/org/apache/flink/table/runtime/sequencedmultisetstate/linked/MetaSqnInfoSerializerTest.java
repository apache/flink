package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Test for {@link MetaSqnInfoSerializer}. */
class MetaSqnInfoSerializerTest extends SerializerTestBase<MetaSqnInfo> {

    @Override
    protected TypeSerializer<MetaSqnInfo> createSerializer() {
        return new MetaSqnInfoSerializer();
    }

    @Override
    protected int getLength() {
        return 2 * Long.SIZE / 8;
    }

    @Override
    protected Class<MetaSqnInfo> getTypeClass() {
        return MetaSqnInfo.class;
    }

    @Override
    protected MetaSqnInfo[] getTestData() {
        return new MetaSqnInfo[] {MetaSqnInfo.of(1L, 2L), MetaSqnInfo.of(1L, 1L)};
    }
}
