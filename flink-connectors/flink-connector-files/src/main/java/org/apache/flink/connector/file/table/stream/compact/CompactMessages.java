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
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util class for all compaction messages.
 *
 * <p>The compaction operator graph is: TempFileWriter|parallel ---(InputFile&EndInputFile)--->
 * CompactCoordinator|non-parallel
 * ---(CompactionUnit&EndCompaction)--->CompactOperator|parallel---(PartitionCommitInfo)--->
 * PartitionCommitter|non-parallel
 *
 * <p>Because the end message is a kind of barrier of record messages, they can only be transmitted
 * in the way of full broadcast in the link from coordinator to compact operator.
 */
@Internal
public class CompactMessages {
    private CompactMessages() {}

    /** The input of compact coordinator. */
    public interface CoordinatorInput extends Serializable {}

    /** A partitioned input file. */
    public static class InputFile implements CoordinatorInput {

        private static final long serialVersionUID = 1L;

        private final String partition;
        private final Path file;

        public InputFile(String partition, Path file) {
            this.partition = partition;
            this.file = file;
        }

        public String getPartition() {
            return partition;
        }

        public Path getFile() {
            return file;
        }
    }

    /** A flag to end checkpoint, coordinator can start coordinating one checkpoint. */
    public static class EndCheckpoint implements CoordinatorInput {

        private static final long serialVersionUID = 1L;

        private final long checkpointId;
        private final int taskId;
        private final int numberOfTasks;

        public EndCheckpoint(long checkpointId, int taskId, int numberOfTasks) {
            this.checkpointId = checkpointId;
            this.taskId = taskId;
            this.numberOfTasks = numberOfTasks;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public int getTaskId() {
            return taskId;
        }

        public int getNumberOfTasks() {
            return numberOfTasks;
        }
    }

    /** The output of compact coordinator. */
    public interface CoordinatorOutput extends Serializable {}

    /** The unit of a single compaction. */
    public static class CompactionUnit implements CoordinatorOutput {

        private static final long serialVersionUID = 1L;

        private final int unitId;
        private final String partition;

        // Store strings to improve serialization performance.
        private final String[] pathStrings;

        public CompactionUnit(int unitId, String partition, List<Path> unit) {
            this.unitId = unitId;
            this.partition = partition;
            this.pathStrings =
                    unit.stream().map(Path::toUri).map(URI::toString).toArray(String[]::new);
        }

        public boolean isTaskMessage(int taskNumber, int taskId) {
            return unitId % taskNumber == taskId;
        }

        public int getUnitId() {
            return unitId;
        }

        public String getPartition() {
            return partition;
        }

        public List<Path> getPaths() {
            return Arrays.stream(pathStrings)
                    .map(URI::create)
                    .map(Path::new)
                    .collect(Collectors.toList());
        }
    }

    /**
     * The output of {@link
     * org.apache.flink.connector.file.table.batch.compact.BatchCompactOperator}.
     */
    public static class CompactOutput implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, List<Path>> compactedFiles;

        public CompactOutput(Map<String, List<Path>> compactedFiles) {
            this.compactedFiles = compactedFiles;
        }

        public Map<String, List<Path>> getCompactedFiles() {
            return compactedFiles;
        }
    }

    /** A flag to end compaction. */
    public static class EndCompaction implements CoordinatorOutput {

        private static final long serialVersionUID = 1L;

        private final long checkpointId;

        public EndCompaction(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        public long getCheckpointId() {
            return checkpointId;
        }
    }
}
