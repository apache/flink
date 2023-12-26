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

package org.apache.flink.connector.file.table.stream.compact;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.EndCompaction;
import org.apache.flink.connector.file.table.utils.CompactFileUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Receives compaction units to do compaction. Send partition commit information after compaction
 * finished.
 *
 * <p>Use {@link BulkFormat} to read and use {@link BucketWriter} to write.
 *
 * <p>STATE: This operator stores expired files in state, after the checkpoint completes
 * successfully, We can ensure that these files will not be used again and they can be deleted from
 * the file system.
 */
@Internal
public class CompactOperator<T> extends AbstractStreamOperator<PartitionCommitInfo>
        implements OneInputStreamOperator<CoordinatorOutput, PartitionCommitInfo>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    public static final String UNCOMPACTED_PREFIX = ".uncompacted-";

    public static final String COMPACTED_PREFIX = "compacted-";

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final CompactReader.Factory<T> readerFactory;
    private final CompactWriter.Factory<T> writerFactory;

    private transient FileSystem fileSystem;

    private transient ListState<Map<Long, List<Path>>> expiredFilesState;
    private transient TreeMap<Long, List<Path>> expiredFiles;
    private transient List<Path> currentExpiredFiles;

    private transient Set<String> partitions;

    public CompactOperator(
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
        this.partitions = new HashSet<>();
        this.fileSystem = fsFactory.get();

        ListStateDescriptor<Map<Long, List<Path>>> metaDescriptor =
                new ListStateDescriptor<>(
                        "expired-files",
                        new MapSerializer<>(
                                LongSerializer.INSTANCE,
                                new ListSerializer<>(
                                        new KryoSerializer<>(Path.class, getExecutionConfig()))));
        this.expiredFilesState = context.getOperatorStateStore().getListState(metaDescriptor);
        this.expiredFiles = new TreeMap<>();
        this.currentExpiredFiles = new ArrayList<>();

        if (context.isRestored()) {
            this.expiredFiles.putAll(this.expiredFilesState.get().iterator().next());
        }
    }

    @Override
    public void processElement(StreamRecord<CoordinatorOutput> element) throws Exception {
        CoordinatorOutput value = element.getValue();
        if (value instanceof CompactionUnit) {
            CompactionUnit unit = (CompactionUnit) value;
            if (unit.isTaskMessage(
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getIndexOfThisSubtask())) {
                String partition = unit.getPartition();
                List<Path> paths = unit.getPaths();
                // create a target file to compact to
                Path targetPath = createCompactedFile(paths);
                // do compaction
                CompactFileUtils.doCompact(
                        fileSystem,
                        partition,
                        paths,
                        targetPath,
                        getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration(),
                        readerFactory,
                        writerFactory);

                this.partitions.add(partition);

                // Only after the current checkpoint is successfully executed can delete
                // the expired files, so as to ensure the existence of the files.
                this.currentExpiredFiles.addAll(paths);
            }
        } else if (value instanceof EndCompaction) {
            endCompaction(((EndCompaction) value).getCheckpointId());
        }
    }

    private void endCompaction(long checkpoint) {
        this.output.collect(
                new StreamRecord<>(
                        new PartitionCommitInfo(
                                checkpoint,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                this.partitions.toArray(new String[0]))));
        this.partitions.clear();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        snapshotState(context.getCheckpointId());
    }

    private void snapshotState(long checkpointId) throws Exception {
        expiredFiles.put(checkpointId, new ArrayList<>(currentExpiredFiles));
        expiredFilesState.update(Collections.singletonList(expiredFiles));
        currentExpiredFiles.clear();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        clearExpiredFiles(checkpointId);
    }

    @Override
    public void endInput() throws Exception {
        endCompaction(Long.MAX_VALUE);
        snapshotState(Long.MAX_VALUE);
        clearExpiredFiles(Long.MAX_VALUE);
    }

    private void clearExpiredFiles(long checkpointId) throws IOException {
        // Don't need these metas anymore.
        NavigableMap<Long, List<Path>> outOfDateMetas = expiredFiles.headMap(checkpointId, true);
        for (List<Path> paths : outOfDateMetas.values()) {
            for (Path meta : paths) {
                fileSystem.delete(meta, true);
            }
        }
        outOfDateMetas.clear();
    }

    private static Path createCompactedFile(List<Path> uncompactedFiles) {
        Path path = convertFromUncompacted(uncompactedFiles.get(0));
        return new Path(path.getParent(), COMPACTED_PREFIX + path.getName());
    }

    public static String convertToUncompacted(String path) {
        return UNCOMPACTED_PREFIX + path;
    }

    public static Path convertFromUncompacted(Path path) {
        Preconditions.checkArgument(
                path.getName().startsWith(UNCOMPACTED_PREFIX),
                "This should be uncompacted file: " + path);
        return new Path(path.getParent(), path.getName().substring(UNCOMPACTED_PREFIX.length()));
    }
}
