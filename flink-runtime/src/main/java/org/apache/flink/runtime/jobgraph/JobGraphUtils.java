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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

/** Utilities for generating {@link JobGraph}. */
public enum JobGraphUtils {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(JobGraphUtils.class);

    public static void addUserArtifactEntries(
            Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts,
            JobGraph jobGraph) {
        if (userArtifacts != null && !userArtifacts.isEmpty()) {
            try {
                java.nio.file.Path tmpDir =
                        Files.createTempDirectory("flink-distributed-cache-" + jobGraph.getJobID());
                for (Tuple2<String, DistributedCache.DistributedCacheEntry> originalEntry :
                        userArtifacts) {
                    Path filePath = new Path(originalEntry.f1.filePath);
                    boolean isLocalDir = false;
                    try {
                        FileSystem sourceFs = filePath.getFileSystem();
                        isLocalDir =
                                !sourceFs.isDistributedFS()
                                        && sourceFs.getFileStatus(filePath).isDir();
                    } catch (IOException ioe) {
                        LOG.warn(
                                "Could not determine whether {} denotes a local path.",
                                filePath,
                                ioe);
                    }
                    // zip local directories because we only support file uploads
                    DistributedCache.DistributedCacheEntry entry;
                    if (isLocalDir) {
                        Path zip =
                                FileUtils.compressDirectory(
                                        filePath,
                                        new Path(tmpDir.toString(), filePath.getName() + ".zip"));
                        entry =
                                new DistributedCache.DistributedCacheEntry(
                                        zip.toString(), originalEntry.f1.isExecutable, true);
                    } else {
                        entry =
                                new DistributedCache.DistributedCacheEntry(
                                        filePath.toString(), originalEntry.f1.isExecutable, false);
                    }
                    jobGraph.addUserArtifact(originalEntry.f0, entry);
                }
            } catch (IOException ioe) {
                throw new FlinkRuntimeException(
                        "Could not compress distributed-cache artifacts.", ioe);
            }
        }
    }
}
