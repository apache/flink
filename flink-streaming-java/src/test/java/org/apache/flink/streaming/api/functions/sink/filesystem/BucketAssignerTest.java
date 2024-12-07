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

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link BucketAssigner bucket assigners}. */
class BucketAssignerTest {

    @TempDir private static java.nio.file.Path tempDir;

    @Test
    void testAssembleBucketPath() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempDir);
        final Path basePath = new Path(outDir.toURI());
        final long time = 1000L;

        final RollingPolicy<String, String> rollingPolicy =
                DefaultRollingPolicy.builder().withMaxPartSize(new MemorySize(7L)).build();

        final Buckets<String, String> buckets =
                new Buckets<>(
                        basePath,
                        new BasePathBucketAssigner<>(),
                        new DefaultBucketFactoryImpl<>(),
                        new RowWiseBucketWriter<>(
                                FileSystem.get(basePath.toUri()).createRecoverableWriter(),
                                new SimpleStringEncoder<>()),
                        rollingPolicy,
                        0,
                        OutputFileConfig.builder().build());

        Bucket<String, String> bucket =
                buckets.onElement("abc", new TestUtils.MockSinkContext(time, time, time));
        assertThat(bucket.getBucketPath()).isEqualTo(new Path(basePath.toUri()));
    }
}
