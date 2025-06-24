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

package org.apache.flink.core.fs.local;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Base class for testing implementations of {@link RecoverableFsDataOutputStream}. */
public abstract class AbstractRecoverableFsDataOutputStreamTest {

    public enum Event {
        CLOSE,
        FLUSH,
        SYNC
    }

    @TempDir Path tmp;

    /**
     * Tests that #closeForCommit leads to a durable write to the temporary file and to target on
     * commit.
     */
    @Test
    void testDurableWriteOnCommit() throws IOException {
        // Setup
        final int seed = 4711;
        final Random random = new Random(seed);
        final byte[] buffer = new byte[4 * 4096];
        final List<LocalRecoverableFsDataOutputStreamTest.Event> testLog = new ArrayList<>();
        final Path target = tmp.resolve("target");
        final Path temp = tmp.resolve("temp");

        Tuple2<RecoverableFsDataOutputStream, Closeable> testInstance =
                createTestInstance(target, temp, testLog);

        // Create test object
        final RecoverableFsDataOutputStream testOutStreamInstance = testInstance.f0;

        // Write test data
        random.nextBytes(buffer);
        testOutStreamInstance.write(buffer);

        // Test closeForCommit
        Assertions.assertTrue(testLog.isEmpty());
        RecoverableFsDataOutputStream.Committer committer = testOutStreamInstance.closeForCommit();
        Assertions.assertEquals(getExpectedResult(), testLog);

        testInstance.f1.close();
        Assertions.assertArrayEquals(buffer, FileUtils.readAllBytes(temp));

        // Test commit
        Assertions.assertFalse(target.toFile().exists());
        committer.commit();
        Assertions.assertTrue(target.toFile().exists());
        Assertions.assertArrayEquals(buffer, FileUtils.readAllBytes(target));
    }

    public abstract Tuple2<RecoverableFsDataOutputStream, Closeable> createTestInstance(
            Path target, Path temp, List<LocalRecoverableFsDataOutputStreamTest.Event> testLog)
            throws IOException;

    public List<Event> getExpectedResult() {
        return Arrays.asList(
                LocalRecoverableFsDataOutputStreamTest.Event.FLUSH,
                LocalRecoverableFsDataOutputStreamTest.Event.SYNC,
                LocalRecoverableFsDataOutputStreamTest.Event.CLOSE);
    }
}
