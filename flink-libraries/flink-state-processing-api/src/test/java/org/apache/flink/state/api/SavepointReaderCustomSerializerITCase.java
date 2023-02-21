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

package org.apache.flink.state.api;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;

/** IT case for reading state. */
public class SavepointReaderCustomSerializerITCase extends SavepointReaderITTestBase {
    private static final ListStateDescriptor<Integer> list =
            new ListStateDescriptor<>(LIST_NAME, CustomIntSerializer.INSTANCE);

    private static final ListStateDescriptor<Integer> union =
            new ListStateDescriptor<>(UNION_NAME, CustomIntSerializer.INSTANCE);

    private static final MapStateDescriptor<Integer, String> broadcast =
            new MapStateDescriptor<>(
                    BROADCAST_NAME, CustomIntSerializer.INSTANCE, StringSerializer.INSTANCE);

    public SavepointReaderCustomSerializerITCase() {
        super(list, union, broadcast);
    }

    @Override
    public DataStream<Integer> readListState(SavepointReader savepoint) throws IOException {
        return savepoint.readListState(
                OperatorIdentifier.forUid(UID), LIST_NAME, Types.INT, CustomIntSerializer.INSTANCE);
    }

    @Override
    public DataStream<Integer> readUnionState(SavepointReader savepoint) throws IOException {
        return savepoint.readUnionState(
                OperatorIdentifier.forUid(UID),
                UNION_NAME,
                Types.INT,
                CustomIntSerializer.INSTANCE);
    }

    @Override
    public DataStream<Tuple2<Integer, String>> readBroadcastState(SavepointReader savepoint)
            throws IOException {
        return savepoint.readBroadcastState(
                OperatorIdentifier.forUid(UID),
                BROADCAST_NAME,
                Types.INT,
                Types.STRING,
                CustomIntSerializer.INSTANCE,
                StringSerializer.INSTANCE);
    }
}
