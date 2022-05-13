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
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link InProgressFileWriter} for row-wise formats that use an {@link Encoder}. This also
 * implements the {@link PartFileInfo} and the {@link OutputStreamBasedCompactingFileWriter}.
 */
@Internal
public final class RowWisePartWriter<IN, BucketID>
        extends OutputStreamBasedPartFileWriter<IN, BucketID> {

    private final Encoder<IN> encoder;

    public RowWisePartWriter(
            final BucketID bucketId,
            final Path path,
            final RecoverableFsDataOutputStream currentPartStream,
            final Encoder<IN> encoder,
            final long creationTime) {
        super(bucketId, path, currentPartStream, creationTime);
        this.encoder = Preconditions.checkNotNull(encoder);
    }

    @Override
    public void write(final IN element, final long currentTime) throws IOException {
        ensureWriteType(Type.RECORD_WISE);
        encoder.encode(element, currentPartStream);
        markWrite(currentTime);
    }
}
