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

package org.apache.flink.table.connector.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** Managed {@link Sink} for testing compaction. */
public class TestManagedSink
        implements Sink<RowData>,
                SupportsCommitter<TestManagedCommittable>,
                SupportsPreCommitTopology<TestManagedCommittable, TestManagedCommittable> {

    private static final long serialVersionUID = 1L;

    private final ObjectIdentifier tableIdentifier;
    private final Path basePath;

    public TestManagedSink(ObjectIdentifier tableIdentifier, Path basePath) {
        this.tableIdentifier = tableIdentifier;
        this.basePath = basePath;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        return new TestManagedSinkWriter();
    }

    @Override
    public Committer<TestManagedCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new TestManagedSinkCommitter(tableIdentifier, basePath);
    }

    @Override
    public SimpleVersionedSerializer<TestManagedCommittable> getCommittableSerializer() {
        return new TestManagedSinkCommittableSerializer();
    }

    @Override
    public DataStream<CommittableMessage<TestManagedCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<TestManagedCommittable>> committables) {
        return committables.global();
    }

    @Override
    public SimpleVersionedSerializer<TestManagedCommittable> getWriteResultSerializer() {
        return new TestManagedSinkCommittableSerializer();
    }
}
