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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.utils.PartitionPathUtils.searchPartSpecAndPaths;

/**
 * Manage temporary files for writing files. Use special rules to organize directories for temporary
 * files.
 *
 * <p>Temporary file directory contains the following directory parts: 1.temporary base path
 * directory. 2.task id directory. 3.directories to specify partitioning. 4.data files. eg:
 * /tmp/task-0-attempt-0/p0=1/p1=2/fileName.
 */
@Internal
public class PartitionTempFileManager {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionTempFileManager.class);

    private static final String TASK_DIR_PREFIX = "task-";
    private static final String ATTEMPT_PREFIX = "attempt-";

    /** <b>ATTENTION:</b> please keep TASK_DIR_FORMAT matching with TASK_DIR_PATTERN. */
    private static final String TASK_DIR_FORMAT = "%s%d-%s%d";

    private static final Pattern TASK_DIR_PATTERN =
            Pattern.compile(TASK_DIR_PREFIX + "(\\d+)-" + ATTEMPT_PREFIX + "(\\d+)");
    private final int taskNumber;
    private final Path taskTmpDir;
    private final OutputFileConfig outputFileConfig;

    private transient int nameCounter = 0;

    public PartitionTempFileManager(
            FileSystemFactory factory, Path tmpPath, int taskNumber, int attemptNumber)
            throws IOException {
        this(factory, tmpPath, taskNumber, attemptNumber, new OutputFileConfig("", ""));
    }

    public PartitionTempFileManager(
            FileSystemFactory factory,
            Path tmpPath,
            int taskNumber,
            int attemptNumber,
            OutputFileConfig outputFileConfig)
            throws IOException {
        this.taskNumber = taskNumber;
        this.outputFileConfig = outputFileConfig;

        // generate task temp dir with task and attempt number like "task-0-attempt-0"
        String taskTmpDirName =
                String.format(
                        TASK_DIR_FORMAT,
                        TASK_DIR_PREFIX,
                        taskNumber,
                        ATTEMPT_PREFIX,
                        attemptNumber);
        this.taskTmpDir = new Path(tmpPath, taskTmpDirName);
        factory.create(taskTmpDir.toUri()).delete(taskTmpDir, true);
    }

    /** Generate a new partition directory with partitions. */
    public Path createPartitionDir(String... partitions) {
        Path parentPath = taskTmpDir;
        for (String dir : partitions) {
            parentPath = new Path(parentPath, dir);
        }
        return new Path(parentPath, newFileName());
    }

    private String newFileName() {
        return String.format(
                "%s-%s-file-%d%s",
                outputFileConfig.getPartPrefix(),
                taskName(taskNumber),
                nameCounter++,
                outputFileConfig.getPartSuffix());
    }

    private static String taskName(int task) {
        return TASK_DIR_PREFIX + task;
    }

    /** Returns task temporary paths in this checkpoint. */
    public static List<Path> listTaskTemporaryPaths(
            FileSystem fs, Path basePath, BiPredicate<Integer, Integer> taskAttemptFilter)
            throws Exception {
        List<Path> taskTmpPaths = new ArrayList<>();

        if (fs.exists(basePath)) {
            for (FileStatus taskStatus : fs.listStatus(basePath)) {
                final String taskDirName = taskStatus.getPath().getName();
                final Matcher matcher = TASK_DIR_PATTERN.matcher(taskDirName);
                if (matcher.matches()) {
                    final int subtaskIndex = Integer.parseInt(matcher.group(1));
                    final int attemptNumber = Integer.parseInt(matcher.group(2));
                    if (taskAttemptFilter.test(subtaskIndex, attemptNumber)) {
                        taskTmpPaths.add(taskStatus.getPath());
                    }
                }
            }
        } else {
            LOG.warn(
                    "The path {} doesn't exist. Maybe no data is generated in the path and the path is not created.",
                    basePath);
        }
        return taskTmpPaths;
    }

    /** Collect all partitioned paths, aggregate according to partition spec. */
    public static Map<LinkedHashMap<String, String>, List<Path>> collectPartSpecToPaths(
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
