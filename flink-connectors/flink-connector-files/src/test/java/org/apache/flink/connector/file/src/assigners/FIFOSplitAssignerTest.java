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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** Unit tests for the {@link FIFOSplitAssigner}. */
public class FIFOSplitAssignerTest {
    private static final Path TEST_PATH =
            Path.fromLocalFile(new File(System.getProperty("java.io.tmpdir")));

    @Test
    public void testAssign() {
        List<FileSourceSplit> fileSourceSplits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            fileSourceSplits.add(createSplit(i, "host" + i));
        }

        FIFOSplitAssigner splitAssigner = new FIFOSplitAssigner(fileSourceSplits);
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(
                    String.valueOf(i), splitAssigner.getNext("host" + i).get().splitId());
        }

        Assert.assertFalse(splitAssigner.getNext("host").isPresent());
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static FileSourceSplit createSplit(int id, String... hosts) {
        return new FileSourceSplit(String.valueOf(id), TEST_PATH, 0, 1024, hosts);
    }
}
