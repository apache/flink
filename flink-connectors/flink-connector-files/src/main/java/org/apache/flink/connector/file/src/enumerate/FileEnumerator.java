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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * The {@code FileEnumerator}'s task is to discover all files to be read and to split them into a
 * set of {@link FileSourceSplit}.
 *
 * <p>This includes possibly, path traversals, file filtering (by name or other patterns) and
 * deciding whether to split files into multiple splits, and how to split them.
 */
@PublicEvolving
public interface FileEnumerator {

    /**
     * Generates all file splits for the relevant files under the given paths. The {@code
     * minDesiredSplits} is an optional hint indicating how many splits would be necessary to
     * exploit parallelism properly.
     */
    Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Factory for the {@code FileEnumerator}, to allow the {@code FileEnumerator} to be eagerly
     * initialized and to not be serializable.
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        FileEnumerator create();
    }
}
