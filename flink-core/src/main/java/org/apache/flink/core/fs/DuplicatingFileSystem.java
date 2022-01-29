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

package org.apache.flink.core.fs;

import java.io.IOException;
import java.util.List;

/**
 * An extension interface for {@link FileSystem FileSystems} that can perform cheap DFS side
 * duplicate operation. Such an operation can improve the time required for creating cheaply
 * independent snapshots from incremental snapshots.
 */
public interface DuplicatingFileSystem {
    /**
     * Tells if we can perform duplicate/copy between given paths.
     *
     * <p>This should be a rather cheap operation, preferably not involving any remote accesses. You
     * can check e.g. if both paths are on the same host.
     *
     * @param source The path of the source file to duplicate
     * @param destination The path where to duplicate the source file
     * @return true, if we can perform the duplication
     */
    boolean canFastDuplicate(Path source, Path destination) throws IOException;

    /**
     * Duplicates the source path into the destination path.
     *
     * <p>You should first check if you can duplicate with {@link #canFastDuplicate(Path, Path)}.
     *
     * @param requests Pairs of src/dst to copy.
     */
    void duplicate(List<CopyRequest> requests) throws IOException;

    /** A pair of source and destination to duplicate a file. */
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
            };
        }
    }
}
