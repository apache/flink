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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/** This output format copies files from an existing savepoint into a new directory. */
@Internal
public final class FileCopyFunction implements OutputFormat<String> {

    private static final long serialVersionUID = 1L;

    private final String path;

    /** @param path the destination path to copy file */
    public FileCopyFunction(String path) {
        this.path = Preconditions.checkNotNull(path, "The destination path cannot be null");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // Create the destination parent directory before copy.
        // It is not a problem if it exists already.
        Path destParent = new Path(path);
        destParent.getFileSystem().mkdirs(destParent);
    }

    @Override
    public void writeRecord(String record) throws IOException {
        Path sourcePath = new Path(record);
        Path destPath = new Path(path, sourcePath.getName());
        try (FSDataOutputStream os =
                        destPath.getFileSystem()
                                .create(destPath, FileSystem.WriteMode.NO_OVERWRITE);
                FSDataInputStream is = sourcePath.getFileSystem().open(sourcePath)) {
            IOUtils.copyBytes(is, os);
        }
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void close() throws IOException {}
}
