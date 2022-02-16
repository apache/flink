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
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RecordWiseFileCompactor.Reader} implementation that reads the file as an {@link
 * FSDataInputStream} and decodes the record with the {@link Decoder}.
 */
@PublicEvolving
public class DecoderBasedReader<T> implements RecordWiseFileCompactor.Reader<T> {
    private final Decoder<T> decoder;

    public DecoderBasedReader(Path path, Decoder<T> decoder) throws IOException {
        this.decoder = checkNotNull(decoder);
        InputStream input = path.getFileSystem().open(path);
        this.decoder.open(input);
    }

    @Override
    public T read() throws IOException {
        return decoder.decodeNext();
    }

    @Override
    public void close() throws Exception {
        decoder.close();
    }

    /**
     * A {@link Decoder} to decode the file content into the actual records.
     *
     * <p>A {@link Decoder} is generally the reverse of a {@link
     * org.apache.flink.api.common.serialization.Encoder}.
     *
     * @param <T> Thy type of the records the reader is reading.
     */
    public interface Decoder<T> extends Serializable {

        /** Prepares to start decoding the input stream. */
        void open(InputStream input) throws IOException;

        /**
         * @return The next record that decoded from the opened input stream, or null if no more
         *     available.
         */
        T decodeNext() throws IOException;

        /** Closes the open resources. The decoder is responsible to close the input stream. */
        void close() throws IOException;

        /** Factory to create {@link Decoder}. */
        interface Factory<T> extends Serializable {
            Decoder<T> create();
        }
    }

    /** Factory for {@link DecoderBasedReader}. */
    public static class Factory<T> implements RecordWiseFileCompactor.Reader.Factory<T> {
        private final Decoder.Factory<T> decoderFactory;

        public Factory(Decoder.Factory<T> decoderFactory) {
            this.decoderFactory = decoderFactory;
        }

        @Override
        public DecoderBasedReader<T> createFor(Path path) throws IOException {
            return new DecoderBasedReader<>(path, decoderFactory.create());
        }
    }
}
