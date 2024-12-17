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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A file mapping entry that encapsulates source and destination path. Source Path : dest Path = 1 :
 * N.
 */
public class MappingEntry extends ReferenceCounted {
    private static final Logger LOG = LoggerFactory.getLogger(MappingEntry.class);

    /** The reference of file mapping manager. */
    private FileSystem fileSystem;

    /** The original path of file. */
    String sourcePath;

    boolean isDirectory;

    /** When delete a directory, if the directory is the parent of this source file, track it. */
    @Nullable MappingEntry parentDir;

    public MappingEntry(FileSystem fileSystem, String sourcePath) {
        this(2, fileSystem, sourcePath, false);
    }

    public MappingEntry(
            int initReference, FileSystem fileSystem, String sourcePath, boolean isDirectory) {
        super(initReference);
        this.fileSystem = fileSystem;
        this.sourcePath = sourcePath;
        this.parentDir = null;
        this.isDirectory = isDirectory;
    }

    public int release() {
        int res = super.release();
        if (parentDir != null) {
            parentDir.release();
        }
        return res;
    }

    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        try {
            fileSystem.delete(new Path(sourcePath), isDirectory);
        } catch (Exception e) {
            LOG.warn("Failed to delete file {}.", sourcePath, e);
        }
    }
}
