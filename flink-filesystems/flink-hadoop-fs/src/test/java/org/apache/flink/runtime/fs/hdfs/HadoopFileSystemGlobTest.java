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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

/** Test for {@link org.apache.hadoop.fs.FileSystem} globStatus API support. */
public class HadoopFileSystemGlobTest {

    private FileSystem mockHadoopFileSystem;
    private HadoopFileSystem testFileSystem;

    @Before
    public void setUp() throws Exception {
        mockHadoopFileSystem = Mockito.mock(FileSystem.class);
        testFileSystem = new HadoopFileSystem(mockHadoopFileSystem);
    }

    @Test
    public void testGlobStatusSupported() {
        assertEquals(testFileSystem.isGlobStatusSupported(), true);
    }

    @Test
    public void testGlobStatusWhenMatchingFilesFound() throws IOException {
        final String globPattern = "hdfs://abc/*";
        final String absFilePath = "hdfs://abc/a";

        org.apache.hadoop.fs.FileStatus mockFileStatusHadoop =
                new org.apache.hadoop.fs.FileStatus();
        mockFileStatusHadoop.setPath(new org.apache.hadoop.fs.Path(absFilePath));
        org.apache.hadoop.fs.FileStatus[] mockFileStatusResult =
                new org.apache.hadoop.fs.FileStatus[] {mockFileStatusHadoop};
        when(mockHadoopFileSystem.globStatus(new org.apache.hadoop.fs.Path(globPattern)))
                .thenReturn(mockFileStatusResult);

        FileStatus[] fileStatusResult = testFileSystem.globStatus(new Path(globPattern));
        assertEquals(fileStatusResult.length, 1);
        assertEquals(fileStatusResult[0].getPath(), new Path(absFilePath));
    }

    @Test
    public void testGlobStatusWhenNoMatchingFilesFound() throws IOException {
        when(mockHadoopFileSystem.globStatus(any())).thenReturn(null);

        FileStatus[] fileStatusResult = testFileSystem.globStatus(new Path("hdfs://abc/*"));
        assertNull(fileStatusResult);
    }

    @Test(expected = IOException.class)
    public void testGlobStatusWhenException() throws IOException {
        when(mockHadoopFileSystem.globStatus(any(org.apache.hadoop.fs.Path.class)))
                .thenThrow(IOException.class);
        testFileSystem.globStatus(new Path("hdfs://abc/*"));
    }
}
