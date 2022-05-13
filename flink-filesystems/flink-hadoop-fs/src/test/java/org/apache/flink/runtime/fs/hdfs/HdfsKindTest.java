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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for extracting the {@link FileSystemKind} from file systems that Flink accesses through
 * Hadoop's File System interface.
 *
 * <p>This class needs to be in this package, because it accesses package private methods from the
 * HDFS file system wrapper class.
 */
public class HdfsKindTest extends TestLogger {

    @Test
    public void testHdfsKind() throws IOException {
        final FileSystem fs = new Path("hdfs://localhost:55445/my/file").getFileSystem();
        assertEquals(FileSystemKind.FILE_SYSTEM, fs.getKind());
    }

    @Test
    public void testS3fileSystemSchemes() {
        assertEquals(FileSystemKind.OBJECT_STORE, HadoopFileSystem.getKindForScheme("s3"));
        assertEquals(FileSystemKind.OBJECT_STORE, HadoopFileSystem.getKindForScheme("s3n"));
        assertEquals(FileSystemKind.OBJECT_STORE, HadoopFileSystem.getKindForScheme("s3a"));
        assertEquals(FileSystemKind.OBJECT_STORE, HadoopFileSystem.getKindForScheme("EMRFS"));
    }

    @Test
    public void testViewFs() {
        assertEquals(FileSystemKind.FILE_SYSTEM, HadoopFileSystem.getKindForScheme("viewfs"));
    }
}
