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

import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.connector.file.table.PartitionTempFileManager.collectPartSpecToPaths;
import static org.apache.flink.connector.file.table.PartitionTempFileManager.listTaskTemporaryPaths;

/** sd. */
public class BatchCompactCoordinator<T>
        extends AbstractStreamOperator<CompactMessages.CoordinatorOutput>
        implements OneInputStreamOperator<T, CompactMessages.CoordinatorOutput>, BoundedOneInput {

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final Path tmpPath;
    private final TableMetaStoreFactory metaStoreFactory;
    private final int partitionColumnSize;

    private transient FileSystem fs;
    private transient TableMetaStoreFactory.TableMetaStore metaStore;

    public BatchCompactCoordinator(
            SupplierWithException<FileSystem, IOException> fsFactory,
            TableMetaStoreFactory metaStoreFactory,
            Path tempPath,
            int partitionColumnSize) {
        this.fsFactory = fsFactory;
        this.tmpPath = tempPath;
        this.partitionColumnSize = partitionColumnSize;
        this.metaStoreFactory = metaStoreFactory;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        fs = fsFactory.get();
        metaStore = metaStoreFactory.createTableMetaStore();
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {}

    @Override
    public void endInput() throws Exception {
        List<Path> taskPaths = listTaskTemporaryPaths(fs, tmpPath);
        AverageSize averageSize = getAverageSize(fs, taskPaths);
        if (averageSize.totalSz / averageSize.numFiles <= 100000) {
            Path basePath = new Path(metaStore.getLocationPath().getPath());
            if (partitionColumnSize > 0) {
                for (Map.Entry<LinkedHashMap<String, String>, List<Path>> entry :
                        collectPartSpecToPaths(fs, taskPaths, partitionColumnSize).entrySet()) {
                    String partition = PartitionPathUtils.generatePartitionPath(entry.getKey());
                    output.collect(
                            new StreamRecord<>(
                                    new CompactMessages.CompactionUnit(
                                            1,
                                            new Path(basePath, partition).getPath(),
                                            collectFiles(entry.getValue()))));
                }
            } else {
                output.collect(
                        new StreamRecord<>(
                                new CompactMessages.CompactionUnit(
                                        1, basePath.getPath(), collectFiles(taskPaths))));
            }
        }
    }

    private AverageSize getAverageSize(FileSystem fs, List<Path> taskPaths) throws IOException {
        long totalSz = 0;
        int numFiles = 0;
        if (partitionColumnSize > 0) {
            for (Path path : taskPaths) {
                FileStatus[] generatedParts =
                        PartitionPathUtils.getFileStatusRecurse(path, partitionColumnSize, fs);
                for (FileStatus fileStatus : generatedParts) {
                    totalSz += fileStatus.getLen();
                    numFiles += 1;
                }
            }
        } else {
            for (Path path : taskPaths) {
                for (FileStatus fileStatus : fs.listStatus(path)) {
                    totalSz += fileStatus.getLen();
                    numFiles += 1;
                }
            }
        }
        return new AverageSize(totalSz, numFiles);
    }

    private List<Path> collectFiles(List<Path> taskPaths) throws IOException {
        List<Path> files = new ArrayList<>();
        for (Path path : taskPaths) {
            files.addAll(
                    Arrays.stream(fs.listStatus(path))
                            .map(FileStatus::getPath)
                            .collect(Collectors.toList()));
        }
        return files;
    }

    private static class AverageSize {
        private final long totalSz;
        private final int numFiles;

        private AverageSize(long totalSz, int numFiles) {
            this.totalSz = totalSz;
            this.numFiles = numFiles;
        }
    }
}
