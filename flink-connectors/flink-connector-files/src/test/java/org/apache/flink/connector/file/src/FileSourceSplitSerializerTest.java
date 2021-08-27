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

package org.apache.flink.connector.file.src;

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Unit tests for the {@link FileSourceSplitSerializer}. */
public class FileSourceSplitSerializerTest {

    @Test
    public void serializeSplitWithHosts() throws Exception {
        final FileSourceSplit split =
                new FileSourceSplit(
                        "random-id",
                        new Path("hdfs://namenode:14565/some/path/to/a/file"),
                        100_000_000,
                        64_000_000,
                        "host1",
                        "host2",
                        "host3");

        final FileSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void serializeSplitWithoutHosts() throws Exception {
        final FileSourceSplit split =
                new FileSourceSplit("some-id", new Path("file:/some/path/to/a/file"), 0, 0);

        final FileSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void serializeSplitWithReaderPosition() throws Exception {
        final FileSourceSplit split =
                new FileSourceSplit(
                        "random-id",
                        new Path("hdfs://namenode:14565/some/path/to/a/file"),
                        100_000_000,
                        64_000_000,
                        new String[] {"host1", "host2", "host3"},
                        new CheckpointedPosition(7665391L, 100L));

        final FileSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void repeatedSerialization() throws Exception {
        final FileSourceSplit split =
                new FileSourceSplit(
                        "an-id", new Path("s3://some-bucket/key/to/the/object"), 0, 1234567);

        serializeAndDeserialize(split);
        serializeAndDeserialize(split);
        final FileSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void repeatedSerializationCaches() throws Exception {
        final FileSourceSplit split =
                new FileSourceSplit(
                        "random-id",
                        new Path("hdfs://namenode:14565/some/path/to/a/file"),
                        100_000_000,
                        64_000_000,
                        "host1",
                        "host2",
                        "host3");

        final byte[] ser1 = FileSourceSplitSerializer.INSTANCE.serialize(split);
        final byte[] ser2 = FileSourceSplitSerializer.INSTANCE.serialize(split);

        assertSame(ser1, ser2);
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    private static FileSourceSplit serializeAndDeserialize(FileSourceSplit split)
            throws IOException {
        final FileSourceSplitSerializer serializer = new FileSourceSplitSerializer();
        final byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    static void assertSplitsEqual(FileSourceSplit expected, FileSourceSplit actual) {
        assertEquals(expected.splitId(), actual.splitId());
        assertEquals(expected.path(), actual.path());
        assertEquals(expected.offset(), actual.offset());
        assertEquals(expected.length(), actual.length());
        assertArrayEquals(expected.hostnames(), actual.hostnames());
        assertEquals(expected.getReaderPosition(), actual.getReaderPosition());
    }
}
