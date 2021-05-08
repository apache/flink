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

package org.apache.flink.connector.file.src.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A simple version of the {@link StreamFormat}, for formats that are not splittable.
 *
 * <p>This format makes no difference between creating readers from scratch (new file) or from a
 * checkpoint. Because of that, if the reader actively checkpoints its position (via the {@link
 * Reader#getCheckpointedPosition()} method) then the checkpointed offset must be a byte offset in
 * the file from which the stream can be resumed as if it were te beginning of the file.
 *
 * <p>For all other details, please check the docs of {@link StreamFormat}.
 *
 * @param <T> The type of records created by this format reader.
 */
@PublicEvolving
public abstract class SimpleStreamFormat<T> implements StreamFormat<T> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new reader. This method is called both for the creation of new reader (from the
     * beginning of a file) and for restoring checkpointed readers.
     *
     * <p>If the reader previously checkpointed an offset, then the input stream will be positioned
     * to that particular offset. Readers checkpoint an offset by returning a value from the method
     * {@link Reader#getCheckpointedPosition()} method with an offset other than {@link
     * CheckpointedPosition#NO_OFFSET}).
     */
    public abstract Reader<T> createReader(Configuration config, FSDataInputStream stream)
            throws IOException;

    /**
     * Gets the type produced by this format. This type will be the type produced by the file source
     * as a whole.
     */
    @Override
    public abstract TypeInformation<T> getProducedType();

    // ------------------------------------------------------------------------
    //  pre-defined methods from Stream Format
    // ------------------------------------------------------------------------

    /** This format is always not splittable. */
    @Override
    public final boolean isSplittable() {
        return false;
    }

    @Override
    public final Reader<T> createReader(
            Configuration config, FSDataInputStream stream, long fileLen, long splitEnd)
            throws IOException {

        checkNotSplit(fileLen, splitEnd);

        final long streamPos = stream.getPos();
        checkArgument(
                streamPos == 0L,
                "SimpleStreamFormat is not splittable, but found non-zero stream position (%s)",
                streamPos);

        return createReader(config, stream);
    }

    @Override
    public final Reader<T> restoreReader(
            final Configuration config,
            final FSDataInputStream stream,
            final long restoredOffset,
            final long fileLen,
            final long splitEnd)
            throws IOException {

        checkNotSplit(fileLen, splitEnd);
        stream.seek(restoredOffset);
        return createReader(config, stream);
    }

    private static void checkNotSplit(long fileLen, long splitEnd) {
        if (splitEnd != fileLen) {
            throw new IllegalArgumentException(
                    String.format(
                            "SimpleStreamFormat is not splittable, but found split end (%d) different from file length (%d)",
                            splitEnd, fileLen));
        }
    }
}
