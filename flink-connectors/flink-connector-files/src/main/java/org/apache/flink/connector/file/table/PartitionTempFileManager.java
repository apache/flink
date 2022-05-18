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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.utils.PartitionPathUtils.searchPartSpecAndPaths;

/**
 * Manage temporary files for writing files. Use special rules to organize directories for temporary
 * files.
 *
 * <p>Temporary file directory contains the following directory parts: 1.temporary base path
 * directory. 2.task id directory. 3.directories to specify partitioning. 4.data files. eg:
 * /tmp/task-0/p0=1/p1=2/fileName.
 */
@Internal
public class PartitionTempFileManager {

    private static final String TASK_DIR_PREFIX = "task-";

    private final int taskNumber;
    private final Path taskTmpDir;
    private final OutputFileConfig outputFileConfig;

    private transient int nameCounter = 0;

    PartitionTempFileManager(FileSystemFactory factory, Path tmpPath, int taskNumber)
            throws IOException {
        this(factory, tmpPath, taskNumber, new OutputFileConfig("", ""));
    }

    PartitionTempFileManager(
            FileSystemFactory factory,
            Path tmpPath,
            int taskNumber,
            OutputFileConfig outputFileConfig)
            throws IOException {
        this.taskNumber = taskNumber;
        this.outputFileConfig = outputFileConfig;

        // generate and clean task temp dir.
        this.taskTmpDir = new Path(tmpPath, TASK_DIR_PREFIX + taskNumber);
        factory.create(taskTmpDir.toUri()).delete(taskTmpDir, true);
    }

    public Path createPartitionFile(String partition, String filePrefix) {
        return new Path(new Path(taskTmpDir, partition), newFileName(filePrefix));
    }

    public Path createPartitionFile(String filePrefix) {
        return new Path(taskTmpDir, newFileName(filePrefix));
    }

    private String newFileName(String filePrefix) {
        return String.format(
                "%s%s-%s-file-%d%s",
                filePrefix,
                outputFileConfig.getPartPrefix(),
                taskName(taskNumber),
                nameCounter++,
                outputFileConfig.getPartSuffix());
    }

    private static boolean isTaskDir(String fileName) {
        return fileName.startsWith(TASK_DIR_PREFIX);
    }

    private static String taskName(int task) {
        return TASK_DIR_PREFIX + task;
    }

    /** Returns task temporary paths in this checkpoint. */
    public static List<Path> listTaskTemporaryPaths(FileSystem fs, Path basePath) throws Exception {
        List<Path> taskTmpPaths = new ArrayList<>();

        for (FileStatus taskStatus : fs.listStatus(basePath)) {
            if (isTaskDir(taskStatus.getPath().getName())) {
                taskTmpPaths.add(taskStatus.getPath());
            }
        }
        return taskTmpPaths;
    }

    /** Collect all partitioned paths, aggregate according to partition spec. */
    static Map<LinkedHashMap<String, String>, List<Path>> collectPartSpecToPaths(
            FileSystem fs, List<Path> taskPaths, int partColSize) {
        Map<LinkedHashMap<String, String>, List<Path>> specToPaths = new HashMap<>();
        for (Path taskPath : taskPaths) {
            searchPartSpecAndPaths(fs, taskPath, partColSize)
                    .forEach(
                            tuple2 ->
                                    specToPaths.compute(
                                            tuple2.f0,
                                            (spec, paths) -> {
                                                paths = paths == null ? new ArrayList<>() : paths;
                                                paths.add(tuple2.f1);
                                                return paths;
                                            }));
        }
        return specToPaths;
    }
}
