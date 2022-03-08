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
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;

/**
 * A {@link RecordWiseFileCompactor.Reader} implementation that reads the file using the {@link
 * FileInputFormat}.
 */
@PublicEvolving
public class InputFormatBasedReader<T> implements RecordWiseFileCompactor.Reader<T> {
    private final Path path;
    private final FileInputFormat<T> inputFormat;

    public InputFormatBasedReader(Path path, FileInputFormat<T> inputFormat) throws IOException {
        this.path = path;
        this.inputFormat = inputFormat;
        open();
    }

    private void open() throws IOException {
        long len = path.getFileSystem().getFileStatus(path).getLen();
        inputFormat.open(new FileInputSplit(0, path, 0, len, null));
    }

    @Override
    public T read() throws IOException {
        if (inputFormat.reachedEnd()) {
            return null;
        }
        return inputFormat.nextRecord(null);
    }

    @Override
    public void close() throws IOException {
        inputFormat.close();
    }

    /** Factory for {@link InputFormatBasedReader}. */
    public static class Factory<T> implements RecordWiseFileCompactor.Reader.Factory<T> {
        private final SerializableSupplierWithException<FileInputFormat<T>, IOException>
                inputFormatFactory;

        public Factory(
                SerializableSupplierWithException<FileInputFormat<T>, IOException>
                        inputFormatFactory) {
            this.inputFormatFactory = inputFormatFactory;
        }

        @Override
        public InputFormatBasedReader<T> createFor(Path path) throws IOException {
            return new InputFormatBasedReader<>(path, inputFormatFactory.get());
        }
    }
}
