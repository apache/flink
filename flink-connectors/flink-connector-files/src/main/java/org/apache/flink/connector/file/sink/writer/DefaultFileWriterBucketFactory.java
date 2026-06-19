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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import java.io.IOException;

/** A factory returning {@link FileWriter writer}. */
@Internal
public class DefaultFileWriterBucketFactory<IN> implements FileWriterBucketFactory<IN> {

    @Override
    public FileWriterBucket<IN> getNewBucket(
            String bucketId,
            Path bucketPath,
            BucketWriter<IN, String> bucketWriter,
            RollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return FileWriterBucket.getNew(
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
    }

    @Override
    public FileWriterBucket<IN> restoreBucket(
            BucketWriter<IN, String> bucketWriter,
            RollingPolicy<IN, String> rollingPolicy,
            FileWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return FileWriterBucket.restore(bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }
}
