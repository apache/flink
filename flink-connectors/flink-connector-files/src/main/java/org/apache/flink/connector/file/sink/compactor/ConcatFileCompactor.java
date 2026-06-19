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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A {@link OutputStreamBasedFileCompactor} implementation that simply concat the compacting files.
 * The fileDelimiter will be added between neighbouring files if provided.
 */
@PublicEvolving
public class ConcatFileCompactor extends OutputStreamBasedFileCompactor {

    private static final int CHUNK_SIZE = 4 * 1024 * 1024;

    private final byte[] fileDelimiter;

    public ConcatFileCompactor() {
        this(null);
    }

    public ConcatFileCompactor(@Nullable byte[] fileDelimiter) {
        this.fileDelimiter = fileDelimiter;
    }

    @Override
    protected void doCompact(List<Path> inputFiles, OutputStream outputStream) throws Exception {
        FileSystem fs = inputFiles.get(0).getFileSystem();
        for (Path input : inputFiles) {
            try (FSDataInputStream inputStream = fs.open(input)) {
                copy(inputStream, outputStream);
            }
            if (fileDelimiter != null) {
                outputStream.write(fileDelimiter);
            }
        }
    }

    private void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[CHUNK_SIZE];
        int length;
        while ((length = in.read(buf)) > 0) {
            out.write(buf, 0, length);
        }
    }
}
