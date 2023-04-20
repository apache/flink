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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestManagedTableFactory;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Managed {@link Committer} for testing compaction. */
public class TestManagedSinkCommitter
        implements GlobalCommitter<TestManagedCommittable, TestManagedCommittable> {

    private final ObjectIdentifier tableIdentifier;
    private final Path basePath;
    private final RowDataEncoder encoder = new RowDataEncoder();

    public TestManagedSinkCommitter(ObjectIdentifier tableIdentifier, Path basePath) {
        this.tableIdentifier = tableIdentifier;
        this.basePath = basePath;
    }

    @Override
    public List<TestManagedCommittable> filterRecoveredCommittables(
            List<TestManagedCommittable> globalCommittables) throws IOException {
        return null;
    }

    @Override
    public TestManagedCommittable combine(List<TestManagedCommittable> committables)
            throws IOException {
        // combine files to add/delete
        return TestManagedCommittable.combine(committables);
    }

    @Override
    public List<TestManagedCommittable> commit(List<TestManagedCommittable> committables)
            throws IOException, InterruptedException {
        for (final TestManagedCommittable combinedCommittable : committables) {
            AtomicReference<Map<CatalogPartitionSpec, List<Path>>> reference =
                    TestManagedTableFactory.MANAGED_TABLE_FILE_ENTRIES.get(tableIdentifier);
            assertThat(reference).isNotNull();
            Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries = reference.get();

            // commit new files
            commitAdd(combinedCommittable.getToAdd(), managedTableFileEntries);

            // cleanup old files
            commitDelete(combinedCommittable.getToDelete(), managedTableFileEntries);

            reference.set(managedTableFileEntries);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}

    private void commitAdd(
            Map<CatalogPartitionSpec, List<RowData>> toAdd,
            Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries)
            throws IOException {

        Map<CatalogPartitionSpec, String> processedPartitions = new HashMap<>();
        for (Map.Entry<CatalogPartitionSpec, List<RowData>> entry : toAdd.entrySet()) {
            CatalogPartitionSpec partitionSpec = entry.getKey();
            String partition =
                    processedPartitions.computeIfAbsent(
                            partitionSpec,
                            (spec) ->
                                    PartitionPathUtils.generatePartitionPath(
                                            new LinkedHashMap<>(spec.getPartitionSpec())));
            List<RowData> elements = entry.getValue();
            Path compactFilePath =
                    new Path(
                            basePath,
                            new Path(
                                    String.format(
                                            "%scompact-%s-file-0", partition, UUID.randomUUID())));
            FSDataOutputStream outputStream =
                    compactFilePath
                            .getFileSystem()
                            .create(compactFilePath, FileSystem.WriteMode.NO_OVERWRITE);
            for (RowData element : elements) {
                encoder.encode(element, outputStream);
            }
            outputStream.flush();
            outputStream.close();

            List<Path> fileEntries = managedTableFileEntries.get(partitionSpec);
            fileEntries.add(compactFilePath);
            managedTableFileEntries.put(partitionSpec, fileEntries);
        }
    }

    private void commitDelete(
            Map<CatalogPartitionSpec, Set<Path>> toDelete,
            Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries)
            throws IOException {
        for (Map.Entry<CatalogPartitionSpec, Set<Path>> entry : toDelete.entrySet()) {
            CatalogPartitionSpec partitionSpec = entry.getKey();
            Set<Path> pathsToDelete = entry.getValue();
            for (Path path : pathsToDelete) {
                path.getFileSystem().delete(path, false);
            }
            List<Path> paths = managedTableFileEntries.get(partitionSpec);
            paths.removeAll(pathsToDelete);
            managedTableFileEntries.put(partitionSpec, paths);
        }
    }

    @Override
    public void endOfInput() throws IOException, InterruptedException {}

    /** An {@link Encoder} implementation to encode records. */
    private static class RowDataEncoder implements Encoder<RowData> {

        private static final long serialVersionUID = 1L;

        private static final byte LINE_DELIMITER = "\n".getBytes(StandardCharsets.UTF_8)[0];

        @Override
        public void encode(RowData rowData, OutputStream stream) throws IOException {
            stream.write(rowData.getString(0).toBytes());
            stream.write(LINE_DELIMITER);
        }
    }
}
