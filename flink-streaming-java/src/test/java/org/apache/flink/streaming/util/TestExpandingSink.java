/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.io.IOException;

/** A test sink that expands into a simple subgraph. Do not use in runtime. */
public class TestExpandingSink
        implements Sink<Integer>,
                WithPreWriteTopology<Integer>,
                WithPreCommitTopology<Integer, Integer>,
                WithPostCommitTopology<Integer, Integer> {

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<Integer>> committables) {
        committables.sinkTo(new DiscardingSink<>());
    }

    @Override
    public DataStream<CommittableMessage<Integer>> addPreCommitTopology(
            DataStream<CommittableMessage<Integer>> committables) {
        return committables.map(value -> value).returns(committables.getType());
    }

    @Override
    public DataStream<Integer> addPreWriteTopology(DataStream<Integer> inputDataStream) {
        return inputDataStream.map(new NoOpIntMap());
    }

    @Override
    public PrecommittingSinkWriter<Integer, Integer> createWriter(InitContext context)
            throws IOException {
        return null;
    }

    @Override
    public Committer<Integer> createCommitter() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<Integer> getCommittableSerializer() {
        return null;
    }
}
