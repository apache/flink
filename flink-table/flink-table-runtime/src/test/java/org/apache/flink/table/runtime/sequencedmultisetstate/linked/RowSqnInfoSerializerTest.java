package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Test for {@link RowSqnInfoSerializer}. */
class RowSqnInfoSerializerTest extends SerializerTestBase<RowSqnInfo> {

    @Override
    protected TypeSerializer<RowSqnInfo> createSerializer() {
        return new RowSqnInfoSerializer();
    }

    @Override
    protected int getLength() {
        return 2 * Long.SIZE / 8;
    }

    @Override
    protected Class<RowSqnInfo> getTypeClass() {
        return RowSqnInfo.class;
    }

    @Override
    protected RowSqnInfo[] getTestData() {
        return new RowSqnInfo[] {RowSqnInfo.of(1L, 2L), RowSqnInfo.of(1L, 1L)};
    }
}
