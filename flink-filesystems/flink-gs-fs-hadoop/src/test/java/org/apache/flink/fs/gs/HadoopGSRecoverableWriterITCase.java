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

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopRecoverableWriterITCase;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Integration tests for {@link org.apache.flink.fs.gs.writer.GSRecoverableWriter}. */
@ExtendWith(RequireGCSConfiguration.class)
class HadoopGSRecoverableWriterITCase extends AbstractHadoopRecoverableWriterITCase {

    @BeforeAll
    static void setup() throws IOException {
        basePath = RequireGCSConfiguration.getBasePath();
        FileSystem.initialize(new Configuration());
        assertThat(basePath.getFileSystem().exists(basePath)).isFalse();
        bigDataChunk = createBigDataChunk(BIG_CHUNK_DATA_PATTERN, 7L << 20);
        skipped = false;
    }

    @Override
    @Disabled("This test is not meaningful. GSRecoverableWriter#cleanupRecoverableState is no-op.")
    public void testCleanupRecoverableState() {}

    @Override
    @Disabled("This test assumes one file for state, but GSRecoverableWriter uses multiple files.")
    public void testCallingDeleteObjectTwiceDoesNotThroughException() {}

    @Override
    protected String getLocalTmpDir() throws Exception {
        return null;
    }

    @Override
    protected String getIncompleteObjectName(RecoverableWriter.ResumeRecoverable recoverable) {
        fail(
                "GCS integration tests should not call this method, because incomplete state may be "
                        + "stored at multiple paths.");
        return null;
    }
}
