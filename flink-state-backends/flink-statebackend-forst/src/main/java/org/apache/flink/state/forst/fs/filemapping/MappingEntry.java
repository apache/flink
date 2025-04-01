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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A file mapping entry that encapsulates source and destination path. Source Path : dest Path = 1 :
 * N.
 */
public class MappingEntry extends ReferenceCounted {

    private static final Logger LOG = LoggerFactory.getLogger(MappingEntry.class);

    MappingEntrySource source;

    FileOwnership fileOwnership;

    final @Nullable FileBasedCache cache;

    final boolean isDirectory;

    volatile boolean writing;

    /** When delete a directory, if the directory is the parent of this source file, track it. */
    @Nullable MappingEntry parentDir;

    public MappingEntry(
            int initReference,
            StreamStateHandle stateHandle,
            FileOwnership fileOwnership,
            boolean isDirectory) {
        this(
                initReference,
                new HandleBackedMappingEntrySource(stateHandle),
                fileOwnership,
                null,
                isDirectory,
                false);
    }

    public MappingEntry(
            int initReference, Path sourcePath, FileOwnership fileOwnership, boolean isDirectory) {
        this(
                initReference,
                new FileBackedMappingEntrySource(sourcePath),
                fileOwnership,
                null,
                isDirectory,
                false);
    }

    public MappingEntry(
            int initReference,
            MappingEntrySource source,
            FileOwnership fileOwnership,
            FileBasedCache cache,
            boolean isDirectory,
            boolean writing) {
        super(initReference);
        this.source = source;
        this.parentDir = null;
        this.fileOwnership = fileOwnership;
        this.cache = cache;
        this.isDirectory = isDirectory;
        this.writing = writing;
        if (!writing && cache != null && !isDirectory && source.cacheable()) {
            try {
                cache.registerInCache(source.getFilePath(), source.getSize());
            } catch (IOException e) {
                LOG.warn("Failed to register file {} in cache.", source, e);
            }
        }
    }

    public void setFileOwnership(FileOwnership ownership) {
        this.fileOwnership = ownership;
    }

    public void setSource(StreamStateHandle stateHandle) {
        if (source instanceof HandleBackedMappingEntrySource) {
            Preconditions.checkArgument(
                    ((HandleBackedMappingEntrySource) source).getStateHandle().equals(stateHandle),
                    "MappingSource is already back by a different StateHandle: %s, the new one is: %s",
                    source,
                    stateHandle);
            return;
        }

        LOG.trace("Set source for file: {}, the source is now backed by: {}", this, stateHandle);
        this.source = new HandleBackedMappingEntrySource(stateHandle);
    }

    public MappingEntrySource getSource() {
        return source;
    }

    public @Nullable Path getSourcePath() {
        return source.getFilePath();
    }

    public FileOwnership getFileOwnership() {
        return fileOwnership;
    }

    public boolean isWriting() {
        return writing;
    }

    public void endWriting() {
        writing = false;
    }

    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        try {
            if (parentDir != null) {
                parentDir.release();
            }
            if (fileOwnership == FileOwnership.NOT_OWNED) {
                // If the source file is not owned by DB, do not delete it.
                return;
            }
            source.delete(isDirectory);
            if (cache != null && !isDirectory && source.cacheable()) {
                cache.delete(source.getFilePath());
            }
        } catch (Exception e) {
            LOG.warn("Failed to delete file {}.", source, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return source.equals(((MappingEntry) o).source);
    }

    @Override
    public String toString() {
        return "MappingEntry{"
                + "source="
                + source
                + ", fileOwnership="
                + fileOwnership
                + ", isDirectory= "
                + isDirectory
                + '}';
    }
}
