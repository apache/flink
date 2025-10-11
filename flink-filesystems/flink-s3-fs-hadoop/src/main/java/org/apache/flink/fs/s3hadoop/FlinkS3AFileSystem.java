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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Extension of Hadoop's S3AFileSystem that exposes the S3Client for use in Flink's recoverable
 * writers.
 *
 * <p>This class simply makes the protected {@link S3AFileSystem#getS3Client()} method public,
 * avoiding the need to use internal S3A APIs.
 */
@Internal
public class FlinkS3AFileSystem extends S3AFileSystem {

    /**
     * Returns the S3Client used by this filesystem.
     *
     * @return the S3Client instance
     */
    @Override
    public S3Client getS3Client() {
        return super.getS3Client();
    }
}
