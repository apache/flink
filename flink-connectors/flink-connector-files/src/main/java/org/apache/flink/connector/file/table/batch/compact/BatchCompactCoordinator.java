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

import org.apache.flink.connector.file.table.BinPacking;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Coordinator for compaction in batch mode. It will collect the written files in {@link
 * BatchFileWriter} and determine whether to compact files or not as well as what files should be
 * merged into a single file.
 *
 * <p>NOTE: The coordination is a stable algorithm, which can ensure different attempts will produce
 * same outputs.
 */
public class BatchCompactCoordinator extends AbstractStreamOperator<CoordinatorOutput>
        implements OneInputStreamOperator<CoordinatorInput, CoordinatorOutput>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final long compactAverageSize;
    private final long compactTargetSize;

    private transient FileSystem fs;
    // the mapping from written partitions to the corresponding files.
    private transient Map<String, List<Path>> inputFiles;

    private transient StreamRecord<CoordinatorOutput> element;

    public BatchCompactCoordinator(
            SupplierWithException<FileSystem, IOException> fsFactory,
            long compactAverageSize,
            long compactTargetSize) {
        this.fsFactory = fsFactory;
        this.compactAverageSize = compactAverageSize;
        this.compactTargetSize = compactTargetSize;
    }

    @Override
    public void open() throws Exception {
        fs = fsFactory.get();
        inputFiles = new HashMap<>();
        element = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<CoordinatorInput> element) throws Exception {
        CoordinatorInput coordinatorInput = element.getValue();
        if (coordinatorInput instanceof InputFile) {
            InputFile file = (InputFile) coordinatorInput;
            // collect the written files
            inputFiles
                    .computeIfAbsent(file.getPartition(), k -> new ArrayList<>())
                    .add(file.getFile());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported input message: " + coordinatorInput);
        }
    }

    @Override
    public void endInput() throws Exception {
        for (Map.Entry<String, List<Path>> partitionFiles : inputFiles.entrySet()) {
            compactPartitionFiles(partitionFiles.getKey(), partitionFiles.getValue());
        }
    }

    @Override
    public void close() throws Exception {
        inputFiles.clear();
    }

    private void compactPartitionFiles(String partition, List<Path> paths) throws IOException {
        if (paths.isEmpty()) {
            return;
        }
        int unitId = 0;
        final Map<Path, Long> filesSize = getFilesSize(fs, paths);
        // calculate the average size of these files
        if (getAverageSize(filesSize) < compactAverageSize) {
            // we should compact
            // get the written files corresponding to the partition
            Function<Path, Long> sizeFunc = filesSize::get;
            // determine what files should be merged to a file
            List<List<Path>> compactUnits = BinPacking.pack(paths, sizeFunc, compactTargetSize);
            for (List<Path> compactUnit : compactUnits) {
                // emit the compact units containing the files path
                output.collect(
                        element.replace(new CompactionUnit(unitId++, partition, compactUnit)));
            }
        } else {
            // no need to merge these files, emit each single file to downstream for committing
            for (Path path : paths) {
                output.collect(
                        element.replace(
                                new CompactionUnit(
                                        unitId++, partition, Collections.singletonList(path))));
            }
        }
    }

    private Map<Path, Long> getFilesSize(FileSystem fs, List<Path> paths) throws IOException {
        Map<Path, Long> filesSize = new HashMap<>();
        for (Path path : paths) {
            long len = fs.getFileStatus(path).getLen();
            filesSize.put(path, len);
        }
        return filesSize;
    }

    private double getAverageSize(Map<Path, Long> filesSize) {
        int numFiles = 0;
        long totalSz = 0;
        for (Map.Entry<Path, Long> fileSize : filesSize.entrySet()) {
            numFiles += 1;
            totalSz += fileSize.getValue();
        }
        return totalSz / (numFiles * 1.0);
    }
}
