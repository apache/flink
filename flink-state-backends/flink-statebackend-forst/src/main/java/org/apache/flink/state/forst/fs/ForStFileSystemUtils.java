/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;

/** Utils for ForStFileSystem. */
public class ForStFileSystemUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ForStFileSystemUtils.class);

    private static final String DUMMY_DIR_NAME = "_dummy_dir_";

    public static boolean isParentDir(@Nullable Path path, String dir) {
        if (path == null) {
            return false;
        }
        return isParentDir(path.toString(), dir);
    }

    public static boolean isParentDir(String path, String dir) {
        if (dir.isEmpty()) {
            return false;
        }
        if (dir.charAt(dir.length() - 1) == '/') {
            return path.startsWith(dir);
        } else {
            return (path.startsWith(dir + "/"));
        }
    }

    public static ForStFlinkFileSystem tryDecorate(ForStFlinkFileSystem fileSystem) {
        try {
            return isIncompleteMkdirEnabled(fileSystem)
                    ? new ForStFileSystemTrackingCreatedDirDecorator(fileSystem)
                    : fileSystem;
        } catch (IOException e) {
            LOG.info("Cannot decorate ForStFlinkFileSystem", e);
        }
        return fileSystem;
    }

    private static boolean isIncompleteMkdirEnabled(ForStFlinkFileSystem fileSystem)
            throws IOException {
        // check if the underlying FileSystem uses an incomplete mkdir implementation
        Path dummyDir = new Path(fileSystem.getRemoteBase(), DUMMY_DIR_NAME + UUID.randomUUID());
        if (fileSystem.mkdirs(dummyDir)) {
            if (!fileSystem.exists(dummyDir)) {
                return true;
            }
            fileSystem.delete(new Path(DUMMY_DIR_NAME), true);
            return false;
        } else {
            LOG.info(
                    "Cannot to mkdir for "
                            + DUMMY_DIR_NAME
                            + ", skip decoration of ForStFlinkFileSystem");
        }
        return false;
    }
}
