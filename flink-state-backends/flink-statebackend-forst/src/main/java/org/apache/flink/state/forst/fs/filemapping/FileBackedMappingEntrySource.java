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
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import javax.annotation.Nonnull;

import java.io.IOException;

/** A {@link MappingEntrySource} backed by a {@link Path}. */
public class FileBackedMappingEntrySource extends MappingEntrySource {
    private final @Nonnull Path filePath;

    FileBackedMappingEntrySource(@Nonnull Path filePath) {
        super();
        this.filePath = filePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileBackedMappingEntrySource that = (FileBackedMappingEntrySource) o;
        return filePath.equals(that.filePath);
    }

    @Override
    public String toString() {
        return "FileBackedSource{" + "filePath=" + filePath + '}';
    }

    @Override
    public void delete(boolean recursive) throws IOException {
        LOG.trace("Delete the file backing a MappingEntry: {}", this);
        filePath.getFileSystem().delete(filePath, recursive);
    }

    @Override
    public @Nonnull Path getFilePath() {
        return filePath;
    }

    @Override
    public long getSize() throws IOException {
        return filePath.getFileSystem().getFileStatus(filePath).getLen();
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        return filePath.getFileSystem().open(filePath);
    }

    @Override
    public FSDataInputStream openInputStream(int bufferSize) throws IOException {
        return filePath.getFileSystem().open(filePath, bufferSize);
    }

    @Override
    public boolean cacheable() {
        return true;
    }

    @Override
    public StreamStateHandle toStateHandle() throws IOException {
        return new FileStateHandle(filePath, getSize());
    }
}
