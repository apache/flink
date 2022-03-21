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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/** Testing append sink. */
public class TestAppendSink implements AppendStreamTableSink<Row> {

    private String[] fNames = null;
    private TypeInformation<?>[] fTypes = null;

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream
                .map(
                        new MapFunction<Row, Tuple2<Boolean, Row>>() {
                            private static final long serialVersionUID = 4671583708680989488L;

                            @Override
                            public Tuple2<Boolean, Row> map(Row value) throws Exception {
                                return Tuple2.of(true, value);
                            }
                        })
                .addSink(new RowSink());
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fTypes, fNames);
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
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        final TestAppendSink copy = new TestAppendSink();
        copy.fNames = fieldNames;
        copy.fTypes = fieldTypes;
        return copy;
    }
}
