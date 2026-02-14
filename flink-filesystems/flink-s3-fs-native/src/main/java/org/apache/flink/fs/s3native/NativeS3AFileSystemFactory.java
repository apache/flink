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

package org.apache.flink.fs.s3native;

/**
 * Factory for the native S3 file system registered for the {@code s3a://} scheme.
 *
 * <p>This provides compatibility with applications that use the Hadoop S3A scheme ({@code s3a://})
 * while using Flink's native S3 implementation built on AWS SDK v2.
 *
 * <p>All configuration options are the same as for the {@code s3://} scheme. See {@link
 * NativeS3FileSystemFactory} for available options.
 */
public class NativeS3AFileSystemFactory extends NativeS3FileSystemFactory {

    @Override
    public String getScheme() {
        return "s3a";
    }
}
