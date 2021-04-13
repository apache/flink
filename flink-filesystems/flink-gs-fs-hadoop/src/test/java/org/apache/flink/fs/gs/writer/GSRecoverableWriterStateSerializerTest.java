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

package org.apache.flink.fs.gs.writer;

import com.google.cloud.storage.BlobId;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Test recoverable writer serializer. */
public class GSRecoverableWriterStateSerializerTest {

    private void shouldSerializeAndDeserializeState(
            String bucketName,
            String objectName,
            long bytesWritten,
            boolean closed,
            int componentObjectIdCount)
            throws IOException {

        // create the state
        BlobId finalBlobId = BlobId.of(bucketName, objectName);
        List<UUID> componentObjectIds = new ArrayList<>();
        for (int i = 0; i < componentObjectIdCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }
        GSRecoverableWriterState state =
                new GSRecoverableWriterState(finalBlobId, bytesWritten, closed, componentObjectIds);

        // serialize and deserialize
        GSRecoverableWriterStateSerializer serializer = GSRecoverableWriterStateSerializer.INSTANCE;
        byte[] serialized = serializer.serialize(state);
        GSRecoverableWriterState deserializedState =
                (GSRecoverableWriterState)
                        serializer.deserialize(serializer.getVersion(), serialized);

        // check that states match
        assertEquals(bucketName, deserializedState.finalBlobId.getBucket());
        assertEquals(objectName, deserializedState.finalBlobId.getName());
        assertNull(deserializedState.finalBlobId.getGeneration());
        assertEquals(bytesWritten, deserializedState.bytesWritten);
        assertEquals(closed, deserializedState.closed);
        assertEquals(componentObjectIdCount, deserializedState.componentObjectIds.size());
        for (int i = 0; i < componentObjectIdCount; i++) {
            assertEquals(
                    state.componentObjectIds.get(i), deserializedState.componentObjectIds.get(i));
        }
    }

    @Test
    public void shouldSerdeOpenState() throws IOException {
        shouldSerializeAndDeserializeState("bucket1", "foo/bar", 1024, false, 3);
    }

    @Test
    public void shouldSerdeClosedState() throws IOException {
        shouldSerializeAndDeserializeState("bucket2", "foo", 2048, true, 2);
    }

    @Test
    public void shouldSerdeStateWithNoComponents() throws IOException {
        shouldSerializeAndDeserializeState("bucket3", "foo/bar/bar", 4096, false, 0);
    }

    @Test(expected = IOException.class)
    public void shouldFailOnMoreRecentSerializedDataVersion() throws IOException {
        GSRecoverableWriterStateSerializer serializer = GSRecoverableWriterStateSerializer.INSTANCE;
        serializer.deserialize(serializer.getVersion() + 1, new byte[] {0});
    }
}
