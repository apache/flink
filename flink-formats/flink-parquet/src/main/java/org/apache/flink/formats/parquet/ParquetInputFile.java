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

package org.apache.flink.formats.parquet;

import org.apache.flink.core.fs.FSDataInputStream;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

/**
 * Parquet {@link InputFile} implementation, {@link #newStream()} call will delegate to Flink {@link
 * FSDataInputStream}.
 */
public class ParquetInputFile implements InputFile {

    private final FSDataInputStream inputStream;
    private final long length;

    public ParquetInputFile(FSDataInputStream inputStream, long length) {
        this.inputStream = inputStream;
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new FSDataInputStreamAdapter(inputStream);
    }

    /**
     * Adapter which makes {@link FSDataInputStream} become compatible with parquet {@link
     * SeekableInputStream}.
     */
    private static class FSDataInputStreamAdapter extends DelegatingSeekableInputStream {

        private final FSDataInputStream inputStream;

        private FSDataInputStreamAdapter(FSDataInputStream inputStream) {
            super(inputStream);
            this.inputStream = inputStream;
        }

        @Override
        public long getPos() throws IOException {
            return inputStream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            inputStream.seek(newPos);
        }
    }
}
