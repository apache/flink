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

package org.apache.flink.fs.s3.common;

import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Collections;

final class TestS3FileSystemFactory extends AbstractS3FileSystemFactory {

    TestS3FileSystemFactory() {
        super(
                "testFs",
                new HadoopConfigLoader(
                        new String[0],
                        new String[0][],
                        "",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        ""));
    }

    @Override
    protected org.apache.hadoop.fs.FileSystem createHadoopFileSystem() {
        return Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
    }

    @Override
    protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        return fsUri;
    }

    @Nullable
    @Override
    protected S3AccessHelper getS3AccessHelper(FileSystem fs) {
        return null;
    }

    @Override
    public String getScheme() {
        return "test";
    }
}
