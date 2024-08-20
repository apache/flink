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
 * This interface is no longer used. Implementing it doesn't have any effect. Please migrate to
 * {@link PathsCopyingFileSystem} which provides the same functionality.
 */
@Deprecated
public interface DuplicatingFileSystem {
    /** Please use {@link PathsCopyingFileSystem#canCopyPaths(Path, Path)}. */
    boolean canFastDuplicate(Path source, Path destination) throws IOException;

    /** Please use {@link PathsCopyingFileSystem#copyFiles(List, ICloseableRegistry)}. */
    void duplicate(List<CopyRequest> requests) throws IOException;

    /** Please use {@link PathsCopyingFileSystem.CopyRequest}. */
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
