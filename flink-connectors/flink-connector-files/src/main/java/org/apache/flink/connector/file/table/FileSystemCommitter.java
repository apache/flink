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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.apache.flink.connector.file.table.PartitionTempFileManager.collectPartSpecToPaths;
import static org.apache.flink.connector.file.table.PartitionTempFileManager.listTaskTemporaryPaths;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/**
 * File system file committer implementation. It moves all files to output path from temporary path.
 *
 * <p>It's used to commit data to FileSystem table in batch mode.
 *
 * <p>Data consistency: 1.For task failure: will launch a new task and create a {@link
 * PartitionTempFileManager}, this will clean previous temporary files (This simple design can make
 * it easy to delete the invalid temporary directory of the task, but it also causes that our
 * directory does not support the same task to start multiple backups to run). 2.For job master
 * commit failure when overwrite: this may result in unfinished intermediate results, but if we try
 * to run job again, the final result must be correct (because the intermediate result will be
 * overwritten). 3.For job master commit failure when append: This can lead to inconsistent data.
 * But, considering that the commit action is a single point of execution, and only moves files and
 * updates metadata, it will be faster, so the probability of inconsistency is relatively small.
 *
 * <p>See: {@link PartitionTempFileManager}. {@link PartitionLoader}.
 */
@Internal
public class FileSystemCommitter {

    private final FileSystemFactory factory;
    private final TableMetaStoreFactory metaStoreFactory;
    private final boolean overwrite;
    private final boolean isToLocal;
    private final Path tmpPath;
    private final int partitionColumnSize;
    private final ObjectIdentifier identifier;
    private final LinkedHashMap<String, String> staticPartitions;
    private final List<PartitionCommitPolicy> policies;

    public FileSystemCommitter(
            FileSystemFactory factory,
            TableMetaStoreFactory metaStoreFactory,
            boolean overwrite,
            Path tmpPath,
            int partitionColumnSize,
            boolean isToLocal,
            ObjectIdentifier identifier,
            LinkedHashMap<String, String> staticPartitions,
            List<PartitionCommitPolicy> policies) {
        this.factory = factory;
        this.metaStoreFactory = metaStoreFactory;
        this.overwrite = overwrite;
        this.tmpPath = tmpPath;
        this.partitionColumnSize = partitionColumnSize;
        this.isToLocal = isToLocal;
        this.identifier = identifier;
        this.staticPartitions = staticPartitions;
        this.policies = policies;
    }

    /** For committing job's output after successful batch job completion. */
    public void commitPartitions() throws Exception {
        commitPartitions((subtaskIndex, attemptNumber) -> true);
    }

    /**
     * Commits the partitions with a filter to filter out invalid task attempt files. In speculative
     * execution mode, there might be some files which do not belong to the finished attempt.
     *
     * @param taskAttemptFilter the filter that accepts subtaskIndex and attemptNumber
     * @throws Exception if partition commitment fails
     */
    public void commitPartitions(BiPredicate<Integer, Integer> taskAttemptFilter) throws Exception {
        FileSystem fs = factory.create(tmpPath.toUri());
        List<Path> taskPaths = listTaskTemporaryPaths(fs, tmpPath, taskAttemptFilter);

        try (PartitionLoader loader =
                new PartitionLoader(
                        overwrite, fs, metaStoreFactory, isToLocal, identifier, policies)) {
            if (partitionColumnSize > 0) {
                if (taskPaths.isEmpty() && !staticPartitions.isEmpty()) {
                    if (partitionColumnSize == staticPartitions.size()) {
                        loader.loadEmptyPartition(this.staticPartitions);
                    }
                } else {
                    for (Map.Entry<LinkedHashMap<String, String>, List<Path>> entry :
                            collectPartSpecToPaths(fs, taskPaths, partitionColumnSize).entrySet()) {
                        loader.loadPartition(entry.getKey(), entry.getValue(), true);
                    }
                }
            } else {
                loader.loadNonPartition(taskPaths, true);
            }
        } finally {
            for (Path taskPath : taskPaths) {
                fs.delete(taskPath, true);
            }
        }
    }

    /**
     * For committing job's output after successful batch job completion, it will commit with the
     * given partitions and corresponding files written which means it'll move the temporary files
     * to partition's location.
     */
    public void commitPartitionsWithFiles(Map<String, List<Path>> partitionsFiles)
            throws Exception {
        FileSystem fs = factory.create(tmpPath.toUri());
        try (PartitionLoader loader =
                new PartitionLoader(
                        overwrite, fs, metaStoreFactory, isToLocal, identifier, policies)) {
            if (partitionColumnSize > 0) {
                if (partitionsFiles.isEmpty() && !staticPartitions.isEmpty()) {
                    if (partitionColumnSize == staticPartitions.size()) {
                        loader.loadEmptyPartition(this.staticPartitions);
                    }
                } else {
                    for (Map.Entry<String, List<Path>> partitionFile : partitionsFiles.entrySet()) {
                        LinkedHashMap<String, String> partSpec =
                                extractPartitionSpecFromPath(new Path(partitionFile.getKey()));
                        loader.loadPartition(partSpec, partitionFile.getValue(), false);
                    }
                }
            } else {
                List<Path> files = new ArrayList<>();
                partitionsFiles.values().forEach(files::addAll);
                loader.loadNonPartition(files, false);
            }
        }
    }
}
