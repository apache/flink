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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * A local file system that a test can degrade at runtime. While degraded, opening and creating
 * files fails; deletes, renames, listings, and existence checks go through.
 *
 * <p>The state is static because the {@code MiniCluster} runs in the test JVM. The {@link Factory}
 * is registered in {@code META-INF/services/org.apache.flink.core.fs.FileSystemFactory}.
 */
public class DegradableFileSystem extends LocalFileSystem {

    public static final String SCHEME = "degradable";

    private static final URI FS_URI = URI.create(SCHEME + ":///");

    // Written by the test thread, read by the cluster's threads.
    private static volatile boolean onlyDeletionsAllowed;

    /** Fails every open and create from now on; deletions and metadata operations go through. */
    public static void allowOnlyDeletions() {
        onlyDeletionsAllowed = true;
    }

    public static void reset() {
        onlyDeletionsAllowed = false;
    }

    /** The URI of the given local directory on this file system. */
    public static String uriOf(java.nio.file.Path localPath) {
        return SCHEME + "://" + localPath.toAbsolutePath();
    }

    @Override
    public URI getUri() {
        return FS_URI;
    }

    /** {@code open(Path, int)} delegates to this method in {@link LocalFileSystem}. */
    @Override
    public FSDataInputStream open(Path f) throws IOException {
        failIfDegraded("open", f);
        return super.open(f);
    }

    @Override
    public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
        failIfDegraded("create", filePath);
        return super.create(filePath, overwrite);
    }

    private static void failIfDegraded(String operation, Path path) throws IOException {
        if (onlyDeletionsAllowed) {
            throw new IOException(
                    String.format("The degraded file system refuses to %s %s.", operation, path));
        }
    }

    /** The factory of the {@link DegradableFileSystem}. */
    public static final class Factory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri) {
            return new DegradableFileSystem();
        }
    }
}
