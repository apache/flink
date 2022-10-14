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

package org.apache.flink.connector.file.table.batch.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.compact.CompactContext;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.file.table.stream.compact.CompactOperator.COMPACTED_PREFIX;

/** Batch Compact Operator. * */
public class BatchCompactOperator<T> extends AbstractStreamOperator<PartitionCommitInfo>
        implements OneInputStreamOperator<CompactMessages.CoordinatorOutput, PartitionCommitInfo>,
                BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final CompactReader.Factory<T> readerFactory;
    private final CompactWriter.Factory<T> writerFactory;

    private transient FileSystem fileSystem;

    public BatchCompactOperator(
            SupplierWithException<FileSystem, IOException> fsFactory,
            CompactReader.Factory<T> readerFactory,
            CompactWriter.Factory<T> writerFactory) {
        this.fsFactory = fsFactory;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        fileSystem = fsFactory.get();
    }

    @Override
    public void processElement(StreamRecord<CompactMessages.CoordinatorOutput> element)
            throws Exception {
        CompactMessages.CoordinatorOutput value = element.getValue();
        if (value instanceof CompactMessages.CompactionUnit) {
            CompactMessages.CompactionUnit unit = (CompactMessages.CompactionUnit) value;
            String partition = unit.getPartition();
            List<Path> paths = unit.getPaths();
            doCompact(partition, paths);
        }
    }

    private void doCompact(String partition, List<Path> paths) throws IOException {
        if (paths.size() == 0) {
            return;
        }

        Map<Path, Long> inputMap = new HashMap<>();
        for (Path path : paths) {
            inputMap.put(path, fileSystem.getFileStatus(path).getLen());
        }

        Path target = createCompactedFile(paths);
        if (fileSystem.exists(target)) {
            return;
        }

        checkExist(paths);

        long startMillis = System.currentTimeMillis();

        boolean success = false;
        if (paths.size() == 1) {
            // optimizer for single file
            success = doSingleFileMove(paths.get(0), target);
        }

        if (!success) {
            doMultiFilesCompact(partition, paths, target);
        }

        Map<Path, Long> targetMap = new HashMap<>();
        targetMap.put(target, fileSystem.getFileStatus(target).getLen());

        double costSeconds = ((double) (System.currentTimeMillis() - startMillis)) / 1000;
        LOG.info(
                "Compaction time cost is '{}S', output per file as following format: name=size(byte), target file is '{}', input files are '{}'",
                costSeconds,
                targetMap,
                inputMap);
    }

    private void checkExist(List<Path> candidates) throws IOException {
        for (Path path : candidates) {
            if (!fileSystem.exists(path)) {
                throw new IOException("Compaction file not exist: " + path);
            }
        }
    }

    private boolean doSingleFileMove(Path src, Path dst) throws IOException {
        // we can do nothing for single file move
        fileSystem.rename(src, dst);
        return true;
    }

    private void doMultiFilesCompact(String partition, List<Path> files, Path dst)
            throws IOException {
        Configuration config =
                getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
        CompactWriter<T> writer =
                writerFactory.create(CompactContext.create(config, fileSystem, partition, dst));

        for (Path path : files) {
            try (CompactReader<T> reader =
                    readerFactory.create(
                            CompactContext.create(config, fileSystem, partition, path))) {
                T record;
                while ((record = reader.read()) != null) {
                    writer.write(record);
                }
            }
        }

        // commit immediately
        writer.commit();
    }

    private static Path createCompactedFile(List<Path> uncompactedFiles) {
        Path path = uncompactedFiles.get(0);
        return new Path(path.getParent(), COMPACTED_PREFIX + path.getName());
    }

    @Override
    public void endInput() throws Exception {}
}
