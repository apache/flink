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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/** Test recoverable writer serializer. */
@RunWith(Parameterized.class)
public class GSResumeRecoverableSerializerTest {

    @Parameterized.Parameter(value = 0)
    public String bucketName;

    @Parameterized.Parameter(value = 1)
    public String objectName;

    @Parameterized.Parameter(value = 2)
    public long position;

    @Parameterized.Parameter(value = 3)
    public boolean closed;

    @Parameterized.Parameter(value = 4)
    public int componentCount;

    @Parameterized.Parameters(
            name = "bucketName={0}, objectName={1}, position={2}, closed={3}, componentCount={4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // resume recoverable for foo/bar at position 0, closed, with 4 component ids
                    {"foo", "bar", 0, true, 4},
                    // resume recoverable for foo2/bar at position 1024, not closed, with 0
                    // component ids
                    {"foo2", "bar", 1024, false, 0},
                    // resume recoverable for foo/bar2 at position 2048, closed, with 8 component
                    // ids
                    {"foo", "bar2", 2048, true, 8},
                    // resume recoverable for foo2/bar2 at position 2048, not closed, with 0
                    // component ids
                    {"foo2", "bar2", 2048, false, 0},
                });
    }

    @Test
    public void shouldSerdeState() throws IOException {

        // create the state
        GSBlobIdentifier finalBlobIdentifier = new GSBlobIdentifier(bucketName, objectName);
        ArrayList<UUID> componentObjectIds = new ArrayList<>();
        for (int i = 0; i < componentCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }
        GSResumeRecoverable state =
                new GSResumeRecoverable(finalBlobIdentifier, componentObjectIds, position, closed);

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
        assertEquals(componentCount, deserializedState.componentObjectIds.size());
        for (int i = 0; i < componentCount; i++) {
            assertEquals(
                    state.componentObjectIds.get(i), deserializedState.componentObjectIds.get(i));
        }
    }
}
