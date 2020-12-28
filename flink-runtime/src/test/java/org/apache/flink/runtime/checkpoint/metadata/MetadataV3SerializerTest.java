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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Various tests for the version 3 format serializer of a checkpoint. */
public class MetadataV3SerializerTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCheckpointWithNoState() throws Exception {
        final Random rnd = new Random();

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;
            final Collection<OperatorState> taskStates = Collections.emptyList();
            final Collection<MasterState> masterStates = Collections.emptyList();

            testCheckpointSerialization(checkpointId, taskStates, masterStates, null);
        }
    }

    @Test
    public void testCheckpointWithOnlyMasterState() throws Exception {
        final Random rnd = new Random();
        final int maxNumMasterStates = 5;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final Collection<OperatorState> operatorStates = Collections.emptyList();

            final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
            final Collection<MasterState> masterStates =
                    CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

            testCheckpointSerialization(checkpointId, operatorStates, masterStates, null);
        }
    }

    @Test
    public void testCheckpointWithOnlyTaskStateForCheckpoint() throws Exception {
        testCheckpointWithOnlyTaskState(null);
    }

    @Test
    public void testCheckpointWithOnlyTaskStateForSavepoint() throws Exception {
        testCheckpointWithOnlyTaskState(temporaryFolder.newFolder().toURI().toString());
    }

    private void testCheckpointWithOnlyTaskState(String basePath) throws Exception {
        final Random rnd = new Random();
        final int maxTaskStates = 20;
        final int maxNumSubtasks = 20;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final int numTasks = rnd.nextInt(maxTaskStates) + 1;
            final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
            final Collection<OperatorState> taskStates =
                    CheckpointTestUtils.createOperatorStates(rnd, basePath, numTasks, numSubtasks);

            final Collection<MasterState> masterStates = Collections.emptyList();

            testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
        }
    }

    @Test
    public void testCheckpointWithMasterAndTaskStateForCheckpoint() throws Exception {
        testCheckpointWithMasterAndTaskState(null);
    }

    @Test
    public void testCheckpointWithMasterAndTaskStateForSavepoint() throws Exception {
        testCheckpointWithMasterAndTaskState(temporaryFolder.newFolder().toURI().toString());
    }

    private void testCheckpointWithMasterAndTaskState(String basePath) throws Exception {
        final Random rnd = new Random();

        final int maxNumMasterStates = 5;
        final int maxTaskStates = 20;
        final int maxNumSubtasks = 20;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final int numTasks = rnd.nextInt(maxTaskStates) + 1;
            final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
            final Collection<OperatorState> taskStates =
                    CheckpointTestUtils.createOperatorStates(rnd, basePath, numTasks, numSubtasks);

            final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
            final Collection<MasterState> masterStates =
                    CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

            testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
        }
    }

    /**
     * Test checkpoint metadata (de)serialization.
     *
     * @param checkpointId The given checkpointId will write into the metadata.
     * @param operatorStates the given states for all the operators.
     * @param masterStates the masterStates of the given checkpoint/savepoint.
     */
    private void testCheckpointSerialization(
            long checkpointId,
            Collection<OperatorState> operatorStates,
            Collection<MasterState> masterStates,
            @Nullable String basePath)
            throws IOException {

        MetadataV3Serializer serializer = MetadataV3Serializer.INSTANCE;

        ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
        DataOutputStream out = new DataOutputViewStreamWrapper(baos);

        CheckpointMetadata metadata =
                new CheckpointMetadata(checkpointId, operatorStates, masterStates);
        MetadataV3Serializer.serialize(metadata, out);
        out.close();

        // The relative pointer resolution in MetadataV2V3SerializerBase currently runs the same
        // code as the file system checkpoint location resolution. Because of that, it needs the
        // a "_metadata" file present. we could change the code to resolve the pointer without doing
        // file I/O, but it is somewhat delicate to reproduce that logic without I/O and the same
        // guarantees
        // to differentiate between the supported options of directory addressing and metadata file
        // addressing.
        // So, better safe than sorry, we do actually do the file system operations in the
        // serializer for now,
        // even if it makes the tests a a tad bit more clumsy
        if (basePath != null) {
            final Path metaPath = new Path(basePath, "_metadata");
            // this is in the temp folder, so it will get automatically deleted
            FileSystem.getLocalFileSystem()
                    .create(metaPath, FileSystem.WriteMode.OVERWRITE)
                    .close();
        }

        byte[] bytes = baos.toByteArray();

        DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
        CheckpointMetadata deserialized =
                serializer.deserialize(in, getClass().getClassLoader(), basePath);
        assertEquals(checkpointId, deserialized.getCheckpointId());
        assertEquals(operatorStates, deserialized.getOperatorStates());

        assertEquals(masterStates.size(), deserialized.getMasterStates().size());
        for (Iterator<MasterState> a = masterStates.iterator(),
                        b = deserialized.getMasterStates().iterator();
                a.hasNext(); ) {
            CheckpointTestUtils.assertMasterStateEquality(a.next(), b.next());
        }
    }
}
