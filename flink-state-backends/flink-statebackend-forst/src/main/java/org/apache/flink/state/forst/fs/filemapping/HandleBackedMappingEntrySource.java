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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link MappingEntrySource} backed by a {@link StreamStateHandle}. */
public class HandleBackedMappingEntrySource extends MappingEntrySource {
    private final @Nonnull StreamStateHandle stateHandle;

    HandleBackedMappingEntrySource(@Nonnull StreamStateHandle stateHandle) {
        super();
        this.stateHandle = stateHandle;
    }

    @Nonnull
    public StreamStateHandle getStateHandle() {
        return stateHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HandleBackedMappingEntrySource that = (HandleBackedMappingEntrySource) o;
        return stateHandle.equals(that.stateHandle);
    }

    @Override
    public String toString() {
        return "HandleBackedSource{" + "stateHandle=" + stateHandle + '}';
    }

    @Override
    public void delete(boolean recursive) throws IOException {
        // do nothing because a state handle must not be owned by DB
    }

    @Override
    @Nullable
    public Path getFilePath() {
        if (stateHandle instanceof FileStateHandle) {
            return ((FileStateHandle) stateHandle).getFilePath();
        } else if (stateHandle instanceof SegmentFileStateHandle) {
            return ((SegmentFileStateHandle) stateHandle).getFilePath();
        } else {
            return null;
        }
    }

    @Override
    public long getSize() throws IOException {
        return stateHandle.getStateSize();
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        return stateHandle.openInputStream();
    }

    @Override
    public FSDataInputStream openInputStream(int bufferSize) throws IOException {
        // ignore buffer size
        return stateHandle.openInputStream();
    }

    @Override
    public boolean cacheable() {
        // todo: support cache for segment file
        return stateHandle instanceof FileStateHandle;
    }

    @Override
    public StreamStateHandle toStateHandle() {
        return stateHandle;
    }
}
