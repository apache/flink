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

/*
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.util.List;

/**
 * An interface marking that given {@link FileSystem} have an optimised path for copying paths
 * instead of using {@link FSDataOutputStream} or {@link FSDataInputStream}.
 */
@Experimental
public interface PathsCopyingFileSystem extends IFileSystem {
    /**
     * A pair of source and destination to duplicate a file.
     *
     * <p>At least one of, either source or destination belongs to this {@link
     * PathsCopyingFileSystem}. One of them can point to the local file system. In other words this
     * request can correspond to either: downloading a file from the remote file system, uploading a
     * file to the remote file system or duplicating a file in the remote file system.
     */
    interface CopyRequest {
        /** The path of the source file to duplicate. */
        Path getSource();

        /** The path where to duplicate the source file. */
        Path getDestination();

        /** A factory method for creating a simple pair of source/destination. */
        static CopyRequest of(Path source, Path destination) {
            return new CopyRequest() {
                @Override
                public Path getSource() {
                    return source;
                }

                @Override
                public Path getDestination() {
                    return destination;
                }

                @Override
                public String toString() {
                    return "CopyRequest{"
                            + "source="
                            + source
                            + ", destination="
                            + destination
                            + '}';
                }
            };
        }
    }

    /**
     * List of {@link CopyRequest} to copy in batch by this {@link PathsCopyingFileSystem}. In case
     * of an exception some files might have been already copied fully or partially. Caller should
     * clean this up. Copy can be interrupted by the {@link CloseableRegistry}.
     */
    void copyFiles(List<CopyRequest> requests, ICloseableRegistry closeableRegistry)
            throws IOException;

    @Override
    default boolean canCopyPaths(Path source, Path destination) throws IOException {
        return true;
    }
}
