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
public class GSCommitRecoverableSerializerTest {

    @Parameterized.Parameter(value = 0)
    public String bucketName;

    @Parameterized.Parameter(value = 1)
    public String objectName;

    @Parameterized.Parameter(value = 2)
    public int componentCount;

    @Parameterized.Parameters(name = "bucketName={0}, objectName={1}, componentCount={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // commit recoverable for foo/bar with 4 component object uuids
                    {"foo", "bar", 4},
                    // commit recoverable for foo2/bar with 0 component object uuids
                    {"foo2", "bar", 0},
                    // commit recoverable for foo/bar2 with 8 component object uuids
                    {"foo", "bar2", 8},
                    // commit recoverable for foo2/bar2 with 0 component object uuids
                    {"foo2", "bar2", 0},
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
        GSCommitRecoverable state =
                new GSCommitRecoverable(finalBlobIdentifier, componentObjectIds);

        // serialize and deserialize
        GSCommitRecoverableSerializer serializer = GSCommitRecoverableSerializer.INSTANCE;
        byte[] serialized = serializer.serialize(state);
        GSCommitRecoverable deserializedState =
                (GSCommitRecoverable) serializer.deserialize(serializer.getVersion(), serialized);

        // check that states match
        assertEquals(bucketName, deserializedState.finalBlobIdentifier.bucketName);
        assertEquals(objectName, deserializedState.finalBlobIdentifier.objectName);
        assertEquals(componentCount, deserializedState.componentObjectIds.size());
        for (int i = 0; i < componentCount; i++) {
            assertEquals(
                    state.componentObjectIds.get(i), deserializedState.componentObjectIds.get(i));
        }
    }
}
