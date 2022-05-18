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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A base class for writing bucket. A bucket is exactly files with same bucket id (the bucket id
 * will be a prefix of the file name) written by the {@link FileSystemOutputFormat}. The class
 * provide the ability to renew output format directly or when bucket id assigned to consumed record
 * changes.
 */
@Internal
public class BaseBucketFileWriter<T> {
    protected final PartitionWriter.Context<T> context;
    protected final PartitionTempFileManager manager;
    protected final BucketIdComputer<T> bucketIdComputer;
    protected OutputFormat<T> currentOutputFormat;
    protected int currentBucketId = -1;

    public BaseBucketFileWriter(
            PartitionWriter.Context<T> context,
            PartitionTempFileManager manager,
            BucketIdComputer<T> bucketIdComputer) {
        this.context = context;
        this.manager = manager;
        this.bucketIdComputer = bucketIdComputer;
    }

    /** Always renew a current writer. */
    protected void forceRenewCurrentOutputFormat(T in, @Nullable String partition)
            throws IOException {
        renewCurrentOutputFormat(bucketIdComputer.getBucketId(in), partition);
    }

    /** Only renew the current writer when the record's bucket id changes. */
    protected void mayRenewCurrentOutputFormat(T in, @Nullable String partition)
            throws IOException {
        int nextBucketId = bucketIdComputer.getBucketId(in);
        if (currentBucketId != nextBucketId) {
            renewCurrentOutputFormat(nextBucketId, partition);
        }
    }

    /** Renew the current writer with the specific bucket id. */
    private void renewCurrentOutputFormat(int nextBucketId, @Nullable String partition)
            throws IOException {
        if (currentOutputFormat != null) {
            currentOutputFormat.close();
        }
        currentOutputFormat = createNewOutputFormat(nextBucketId, partition);
        // renew current bucket id
        currentBucketId = nextBucketId;
    }

    protected OutputFormat<T> createNewOutputFormat(int bucketId, @Nullable String partition)
            throws IOException {
        String bucketFilePrefix = bucketIdComputer.getBucketFilePrefix(bucketId);
        Path partitionPath =
                partition == null
                        ? manager.createPartitionFile(bucketFilePrefix)
                        : manager.createPartitionFile(partition, bucketFilePrefix);
        return context.createNewOutputFormat(partitionPath);
    }

    public void close() throws Exception {
        if (currentOutputFormat != null) {
            currentOutputFormat.close();
            currentOutputFormat = null;
        }
    }
}
