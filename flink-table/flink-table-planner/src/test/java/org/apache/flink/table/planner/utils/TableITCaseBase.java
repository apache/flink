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

package org.apache.flink.table.planner.utils;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/** Table IT Case base to use {@link RegisterExtension}. */
public abstract class TableITCaseBase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir protected Path tmpDir;

    protected String getTempFilePath(String fileName) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        return f.toURI().toString();
    }

    protected File createTempFolder() throws IOException {
        return TempDirUtils.newFolder(tmpDir);
    }

    protected File createTempFile() throws IOException {
        Path tmpDirPath = createTempFolder().toPath();
        return TempDirUtils.newFile(tmpDirPath);
    }

    protected File createTempFile(String fileName) throws IOException {
        Path tmpDirPath = createTempFolder().toPath();
        return TempDirUtils.newFile(tmpDirPath, fileName);
    }

    protected String createTempFile(String fileName, String contents) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        FileUtils.writeFileUtf8(f, contents);
        return f.toURI().toString();
    }

    protected File createAndRegisterTempFile(String fileName) throws IOException {
        return new File(TempDirUtils.newFolder(tmpDir), fileName);
    }
}
