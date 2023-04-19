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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/** Batch test base to use {@link RegisterExtension}. */
public class BatchAbstractTestBase {

    public static final int DEFAULT_PARALLELISM = 3;

    @RegisterExtension
    public static MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    @TempDir public static Path tmpDir;

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("100m"));
        return config;
    }

    public static File createTempFolder() throws IOException {
        return TempDirUtils.newFolder(BatchAbstractTestBase.tmpDir);
    }

    public static File createTempFile() throws IOException {
        Path tmpDirPath = createTempFolder().toPath();
        return TempDirUtils.newFile(tmpDirPath);
    }

    public static File createFileInTempFolder(String fileName) throws IOException {
        Path tmpDirPath = createTempFolder().toPath();
        return TempDirUtils.newFile(tmpDirPath, fileName);
    }
}
