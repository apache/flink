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

import org.apache.flink.fs.gs.storage.GSBlobIdentifier;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/** Test recoverable writer serializer. */
public class GSResumeRecoverableSerializerTest {

    private void shouldSerializeAndDeserializeState(
            String bucketName,
            String objectName,
            long position,
            boolean closed,
            int componentObjectIdCount)
            throws IOException {

        // create the state
        GSBlobIdentifier finalBlobIdentifier = new GSBlobIdentifier(bucketName, objectName);
        ArrayList<UUID> componentObjectIds = new ArrayList<>();
        for (int i = 0; i < componentObjectIdCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }
        GSResumeRecoverable state =
                new GSResumeRecoverable(finalBlobIdentifier, position, closed, componentObjectIds);

        // serialize and deserialize
        GSResumeRecoverableSerializer serializer = GSResumeRecoverableSerializer.INSTANCE;
        byte[] serialized = serializer.serialize(state);
        GSResumeRecoverable deserializedState =
                (GSResumeRecoverable) serializer.deserialize(serializer.getVersion(), serialized);

        // check that states match
        assertEquals(bucketName, deserializedState.finalBlobIdentifier.bucketName);
        assertEquals(objectName, deserializedState.finalBlobIdentifier.objectName);
        assertEquals(position, deserializedState.position);
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
        GSResumeRecoverableSerializer serializer = GSResumeRecoverableSerializer.INSTANCE;
        serializer.deserialize(serializer.getVersion() + 1, new byte[] {0});
    }
}
