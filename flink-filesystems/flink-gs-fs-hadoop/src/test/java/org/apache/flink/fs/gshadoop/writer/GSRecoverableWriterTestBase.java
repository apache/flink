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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.gshadoop.GSFileSystemFactory;
import org.apache.flink.fs.gshadoop.MockGSFileSystemHelper;
import org.apache.flink.util.TestLogger;

import org.junit.Before;

import java.net.URI;
import java.util.Random;

/** Base class for GS tests. */
class GSRecoverableWriterTestBase extends TestLogger {

    protected static final Random RANDOM = new Random();

    protected static final String FINAL_BUCKET_NAME = "FINAL_BUCKET_NAME";
    protected static final String FINAL_OBJECT_NAME = "FINAL_OBJECT_NAME";
    protected static final String TEMP_BUCKET_NAME = "TEMP_BUCKET_NAME";
    protected static final URI FINAL_URI =
            URI.create(String.format("gs://%s/%s", FINAL_BUCKET_NAME, FINAL_OBJECT_NAME));
    protected static final Path FINAL_PATH = new Path(FINAL_URI);

    // mocks
    protected MockGSFileSystemHelper fileSystemHelper;
    protected MockGSRecoverableWriterHelper recoverableWriterHelper;

    // the options to use for tests
    protected GSRecoverableOptions recoverableOptions;

    // the recoverable writer being tested
    protected GSRecoverableWriter recoverableWriter;

    @Before
    public void setup() {
        fileSystemHelper = new MockGSFileSystemHelper();
        recoverableWriterHelper =
                (MockGSRecoverableWriterHelper) fileSystemHelper.getRecoverableWriterHelper();
        recoverableOptions =
                new GSRecoverableOptions(
                        fileSystemHelper.getRecoverableWriterHelper(),
                        GSFileSystemFactory.DEFAULT_UPLOAD_CONTENT_TYPE,
                        TEMP_BUCKET_NAME,
                        GSFileSystemFactory.DEFAULT_UPLOAD_TEMP_PREFIX,
                        0);
        recoverableWriter = new GSRecoverableWriter(recoverableOptions);
    }
}
