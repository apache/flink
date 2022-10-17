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

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.connector.file.table.BinPacking;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Coordinator for compaction in batch mode. It will collect the written files in {@link
 * BatchFileWriter} and determine whether to compact files nor not as well as what files should be
 * merged into a single file.
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
    private transient int mergeTargetFileNum = 0;

    public BatchCompactCoordinator(
            SupplierWithException<FileSystem, IOException> fsFactory,
            long compactAverageSize,
            long compactTargetSize) {
        this.fsFactory = fsFactory;
        this.compactAverageSize = compactAverageSize;
        this.compactTargetSize = compactTargetSize;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        fs = fsFactory.get();
        inputFiles = new HashMap<>();
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
        }
    }

    @Override
    public void endInput() throws Exception {
        for (Map.Entry<String, List<Path>> partitionFiles : inputFiles.entrySet()) {
            compactPartitionFiles(partitionFiles.getKey(), partitionFiles.getValue());
        }
        adjustMetricAQE(Math.max(1, mergeTargetFileNum));
    }

    private void compactPartitionFiles(String partition, List<Path> paths) throws IOException {
        int unitId = 0;
        final Map<Path, Long> filesSize = getFilesSize(fs, paths);
        // calculate the average size of these files
        AverageSize averageSize = getAverageSize(filesSize);
        if (averageSize.isLessThan(compactAverageSize)) {
            // we should compact
            // get the written files corresponding to the partition
            Function<Path, Long> sizeFunc = filesSize::get;
            // determine what files should be merged to a file
            List<List<Path>> compactUnits = BinPacking.pack(paths, sizeFunc, compactTargetSize);
            mergeTargetFileNum += (int) compactUnits.stream().filter(f -> f.size() > 1).count();
            for (List<Path> compactUnit : compactUnits) {
                // emit the compact units containing the files path
                output.collect(
                        new StreamRecord<>(new CompactionUnit(unitId++, partition, compactUnit)));
            }
        } else {
            // no need to merge these files, emit each single file to downstream for committing
            for (Path path : paths) {
                output.collect(
                        new StreamRecord<>(
                                new CompactionUnit(
                                        unitId++, partition, Collections.singletonList(path))));
            }
        }
    }

    private Map<Path, Long> getFilesSize(FileSystem fs, List<Path> paths) throws IOException {
        Map<Path, Long> filesStatus = new HashMap<>();
        for (Path path : paths) {
            long len = fs.getFileStatus(path).getLen();
            filesStatus.put(path, len);
        }
        return filesStatus;
    }

    private AverageSize getAverageSize(Map<Path, Long> filesSize) {
        int numFiles = 0;
        long totalSz = 0;
        for (Map.Entry<Path, Long> fileSize : filesSize.entrySet()) {
            numFiles += 1;
            totalSz += fileSize.getValue();
        }
        return new AverageSize(totalSz, numFiles);
    }

    private static class AverageSize {
        private final long totalSz;
        private final int numFiles;

        private AverageSize(long totalSz, int numFiles) {
            this.totalSz = totalSz;
            this.numFiles = numFiles;
        }

        private boolean isLessThan(long averageSize) {
            return numFiles > 0 && totalSz / numFiles < averageSize;
        }
    }

    // hack logic
    private void adjustMetricAQE(int expectParallelism) throws Exception {
        int parallelism = normalizeParallelism(expectParallelism);
        long dataVolumePerTask =
                getContainingTask()
                        .getJobConfiguration()
                        .get(JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK)
                        .getBytes();
        // parallelism =  ceil(numBytes / dataVolumePerTask),
        // since the operator will emit other element which increase numBytes, so we only need to
        // make the numBytes increased forcibly in here is dataVolumePerTask * (parallelism - 1).
        // so that the parallelism inferred by aqe will be the expectParallelism
        long increaseNumBytes = dataVolumePerTask * (parallelism - 1);
        Map<IntermediateResultPartitionID, Counter> numBytesProducedOfPartitions =
                getNumBytesProducedOfPartitions();
        for (Map.Entry<IntermediateResultPartitionID, Counter> entry :
                numBytesProducedOfPartitions.entrySet()) {
            entry.getValue().inc(increaseNumBytes);
        }
    }

    private Map<IntermediateResultPartitionID, Counter> getNumBytesProducedOfPartitions()
            throws Exception {
        TaskIOMetricGroup taskIOMetricGroup =
                getContainingTask().getEnvironment().getMetricGroup().getIOMetricGroup();
        Field field = taskIOMetricGroup.getClass().getDeclaredField("numBytesProducedOfPartitions");
        field.setAccessible(true);
        return (Map<IntermediateResultPartitionID, Counter>) field.get(taskIOMetricGroup);
    }

    static int normalizeParallelism(int parallelism) {
        int down = MathUtils.roundDownToPowerOf2(parallelism);
        int up = MathUtils.roundUpToPowerOfTwo(parallelism);
        return parallelism < (up + down) / 2 ? down : up;
    }
}
