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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionTempFileManager}. */
class PartitionTempFileManagerTest {

    @TempDir private java.nio.file.Path tmpPath;

    @Test
    void testListTaskTemporaryPaths() throws Exception {
        // only accept task-0-attempt-1
        final BiPredicate<Integer, Integer> taskAttemptFilter =
                (subtaskIndex, attemptNumber) -> subtaskIndex == 0 && attemptNumber == 1;

        final FileSystem fs = FileSystem.get(tmpPath.toUri());
        fs.mkdirs(new Path(tmpPath.toUri() + "/task-0-attempt-0")); // invalid attempt number
        fs.mkdirs(new Path(tmpPath.toUri() + "/task-0-attempt-1")); // valid
        fs.mkdirs(new Path(tmpPath.toUri() + "/task-1-attempt-0")); // invalid subtask index
        fs.mkdirs(new Path(tmpPath.toUri() + "/.task-0-attempt-1")); // invisible dir
        fs.mkdirs(new Path(tmpPath.toUri() + "/_SUCCESS")); // not a task dir

        final List<Path> taskTmpPaths =
                PartitionTempFileManager.listTaskTemporaryPaths(
                        fs, new Path(tmpPath.toUri()), taskAttemptFilter);
        final List<String> taskDirs =
                taskTmpPaths.stream().map(Path::getName).collect(Collectors.toList());
        assertThat(taskDirs).hasSize(1).containsExactly("task-0-attempt-1");
    }
}
