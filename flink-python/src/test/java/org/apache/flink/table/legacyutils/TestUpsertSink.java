/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.legacyutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import static org.assertj.core.api.Assertions.assertThat;

/** testing upsert sink. */
public class TestUpsertSink implements UpsertStreamTableSink<Row> {

    private String[] fNames = null;
    private TypeInformation<?>[] fTypes = null;

    private final String[] expectedKeys;
    private final boolean expectedIsAppendOnly;

    public TestUpsertSink(String[] expectedKeys, boolean expectedIsAppendOnly) {
        this.expectedKeys = expectedKeys;
        this.expectedIsAppendOnly = expectedIsAppendOnly;
    }

    @Override
    public void setKeyFields(String[] keys) {
        assertThat(keys)
                .as("Provided key fields do not match expected keys")
                .containsExactlyInAnyOrder(expectedKeys);
    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {
        assertThat(isAppendOnly)
                .as("Provided isAppendOnly does not match expected isAppendOnly")
                .isEqualTo(expectedIsAppendOnly);
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fTypes, fNames);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> s) {
        return s.addSink(new RowSink());
    }

    @Override
    public String[] getFieldNames() {
        return fNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(
            String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        final TestUpsertSink copy = new TestUpsertSink(expectedKeys, expectedIsAppendOnly);
        copy.fNames = fieldNames;
        copy.fTypes = fieldTypes;
        return copy;
    }
}
