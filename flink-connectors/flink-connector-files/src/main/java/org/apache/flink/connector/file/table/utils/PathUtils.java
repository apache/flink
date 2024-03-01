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

package org.apache.flink.connector.file.table.utils;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.UUID;

/** Path utils for file system. */
public class PathUtils {

    public static Path getStagingPath(Path path) {
        // Add a random UUID to prevent multiple sinks from sharing the same staging dir.
        // Please see FLINK-29114 for more details
        Path stagingDir =
                new Path(
                        path,
                        String.join(
                                "_",
                                ".staging_",
                                String.valueOf(System.currentTimeMillis()),
                                UUID.randomUUID().toString()));
        try {
            FileSystem fs = stagingDir.getFileSystem();
            Preconditions.checkState(
                    fs.mkdirs(stagingDir), "Failed to create staging dir " + stagingDir);
            return stagingDir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
