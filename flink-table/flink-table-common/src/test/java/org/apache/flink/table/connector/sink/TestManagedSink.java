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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Managed {@link Sink} for testing compaction. */
public class TestManagedSink implements Sink<RowData, TestManagedCommittable, Void, Void> {

    private static final long serialVersionUID = 1L;

    private final ObjectIdentifier tableIdentifier;
    private final Path basePath;

    public TestManagedSink(ObjectIdentifier tableIdentifier, Path basePath) {
        this.tableIdentifier = tableIdentifier;
        this.basePath = basePath;
    }

    @Override
    public SinkWriter<RowData, TestManagedCommittable, Void> createWriter(
            InitContext context, List<Void> states) throws IOException {
        return new TestManagedSinkWriter();
    }

    @Override
    public Optional<Committer<TestManagedCommittable>> createCommitter() {
        return Optional.of(new TestManagedSinkCommitter(tableIdentifier, basePath));
    }

    @Override
    public Optional<SimpleVersionedSerializer<TestManagedCommittable>> getCommittableSerializer() {
        return Optional.of(new TestManagedSinkCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<TestManagedCommittable, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Collection<String> getCompatibleStateNames() {
        return Collections.emptyList();
    }
}
