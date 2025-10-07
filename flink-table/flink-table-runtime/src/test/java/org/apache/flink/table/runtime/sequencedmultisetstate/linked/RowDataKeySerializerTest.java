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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.IntType;

import java.util.Objects;

/** Test for {@link RowDataKeySerializer}. */
public class RowDataKeySerializerTest extends SerializerTestBase<RowDataKey> {

    private final TestRecordEqualiser equaliser = new TestRecordEqualiser();

    @Override
    protected TypeSerializer<RowDataKey> createSerializer() {
        return new RowDataKeySerializer(
                new RowDataSerializer(new IntType()),
                equaliser,
                equaliser,
                EQUALISER,
                HASH_FUNCTION);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<RowDataKey> getTypeClass() {
        return RowDataKey.class;
    }

    @Override
    protected RowDataKey[] getTestData() {
        return new RowDataKey[] {new RowDataKey(StreamRecordUtils.row(123), equaliser, equaliser)};
    }

    static final GeneratedRecordEqualiser EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    static final GeneratedHashFunction HASH_FUNCTION =
            new GeneratedHashFunction("", "", new Object[0], new Configuration()) {
                @Override
                public HashFunction newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    private static class TestRecordEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind() && row1.getInt(0) == row2.getInt(0);
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(rd.getRowKind(), rd.getInt(0));
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestRecordEqualiser;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}
