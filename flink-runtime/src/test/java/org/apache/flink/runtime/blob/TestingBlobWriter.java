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
 * limitations under the License
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link BlobWriter}. */
public class TestingBlobWriter implements BlobWriter {

    private final Map<Tuple2<JobID, PermanentBlobKey>, byte[]> blobs;
    private int minOffloadingSize;

    public TestingBlobWriter() {
        this(0);
    }

    public TestingBlobWriter(int minOffloadingSize) {
        this.blobs = new HashMap<>();
        this.minOffloadingSize = minOffloadingSize;
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException {
        PermanentBlobKey blobKey = new PermanentBlobKey();
        Tuple2<JobID, PermanentBlobKey> tupleKey = Tuple2.of(jobId, blobKey);
        if (blobs.containsKey(tupleKey)) {
            throw new IllegalStateException("Duplicated key found in in memory blob");
        }
        blobs.put(tupleKey, value);
        return blobKey;
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            int length;
            byte[] buffer = new byte[1024];

            while ((length = inputStream.read(buffer, 0, buffer.length)) != -1) {
                baos.write(buffer, 0, length);
            }
            return this.putPermanent(jobId, baos.toByteArray());
        }
    }

    @Override
    public boolean deletePermanent(JobID jobId, PermanentBlobKey blobKey) {
        return blobs.remove(Tuple2.of(jobId, blobKey)) != null;
    }

    @Override
    public int getMinOffloadingSize() {
        return minOffloadingSize;
    }

    public void setMinOffloadingSize(int minOffloadingSize) {
        this.minOffloadingSize = minOffloadingSize;
    }

    public byte[] getBlob(JobID jobId, PermanentBlobKey blobKey) {
        return blobs.get(Tuple2.of(jobId, blobKey));
    }

    public int numberOfBlobs() {
        return blobs.size();
    }
}
