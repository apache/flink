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

import org.apache.flink.core.fs.FileSystemKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for extracting the {@link FileSystemKind} from file systems that Flink accesses through
 * Hadoop's File System interface.
 *
 * <p>This class needs to be in this package, because it accesses package private methods from the
 * HDFS file system wrapper class.
 */
class HdfsKindTest {
    @Test
    void testS3fileSystemSchemes() {
        assertThat(HadoopFileSystem.getKindForScheme("s3")).isEqualTo(FileSystemKind.OBJECT_STORE);
        assertThat(HadoopFileSystem.getKindForScheme("s3n")).isEqualTo(FileSystemKind.OBJECT_STORE);
        assertThat(HadoopFileSystem.getKindForScheme("s3a")).isEqualTo(FileSystemKind.OBJECT_STORE);
        assertThat(HadoopFileSystem.getKindForScheme("EMRFS"))
                .isEqualTo(FileSystemKind.OBJECT_STORE);
    }

    @Test
    void testViewFs() {
        assertThat(HadoopFileSystem.getKindForScheme("viewfs"))
                .isEqualTo(FileSystemKind.FILE_SYSTEM);
    }
}
