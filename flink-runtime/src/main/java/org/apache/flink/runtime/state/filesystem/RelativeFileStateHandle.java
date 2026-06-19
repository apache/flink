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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;

/**
 * A {@link StreamStateHandle} for state that was written to a file stream. The differences between
 * {@link FileStateHandle} and {@link RelativeFileStateHandle} is that {@link
 * RelativeFileStateHandle} contains relativePath for the given handle.
 */
public class RelativeFileStateHandle extends FileStateHandle {
    private static final long serialVersionUID = 1L;

    private final String relativePath;

    public RelativeFileStateHandle(Path path, String relativePath, long stateSize) {
        super(path, stateSize);
        this.relativePath = relativePath;
    }

    public String getRelativePath() {
        return relativePath;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof RelativeFileStateHandle)) {
            return false;
        }

        RelativeFileStateHandle other = (RelativeFileStateHandle) o;
        return super.equals(o) && relativePath.equals(other.relativePath);
    }

    @Override
    public int hashCode() {
        return 17 * super.hashCode() + relativePath.hashCode();
    }

    @Override
    public String toString() {
        return String.format(
                "RelativeFileStateHandle State: %s, %s [%d bytes]",
                getFilePath(), relativePath, getStateSize());
    }
}
