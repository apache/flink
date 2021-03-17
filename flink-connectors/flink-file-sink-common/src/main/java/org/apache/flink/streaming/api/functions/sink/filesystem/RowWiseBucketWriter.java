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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

/**
 * A factory that creates {@link RowWisePartWriter RowWisePartWriters}.
 *
 * @param <IN> The type of input elements.
 * @param <BucketID> The type of ids for the buckets, as returned by the {@link BucketAssigner}.
 */
@Internal
public class RowWiseBucketWriter<IN, BucketID>
        extends OutputStreamBasedPartFileWriter.OutputStreamBasedBucketWriter<IN, BucketID> {

    private final Encoder<IN> encoder;

    public RowWiseBucketWriter(
            final RecoverableWriter recoverableWriter, final Encoder<IN> encoder) {
        super(recoverableWriter);
        this.encoder = encoder;
    }

    @Override
    public InProgressFileWriter<IN, BucketID> resumeFrom(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final RecoverableWriter.ResumeRecoverable resumable,
            final long creationTime) {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(resumable);

        return new RowWisePartWriter<>(bucketId, stream, encoder, creationTime);
    }

    @Override
    public InProgressFileWriter<IN, BucketID> openNew(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final Path path,
            final long creationTime) {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(path);

        return new RowWisePartWriter<>(bucketId, stream, encoder, creationTime);
    }
}
