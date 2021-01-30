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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests the GS recoverable serializer. */
public class GSRecoverableSerializerTest extends GSRecoverableWriterTestBase {

    @Test
    public void testResumeRecoverableSerde() throws IOException {

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableWriter.ResumeRecoverable resumeRecoverable1 = outputStream.persist();

        SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> serializer =
                recoverableWriter.getResumeRecoverableSerializer();
        byte[] bytes = serializer.serialize(resumeRecoverable1);
        RecoverableWriter.ResumeRecoverable resumeRecoverable2 =
                serializer.deserialize(serializer.getVersion(), bytes);

        GSRecoverable recoverable1 = (GSRecoverable) resumeRecoverable1;
        GSRecoverable recoverable2 = (GSRecoverable) resumeRecoverable2;

        assertEquals(recoverable1.position, recoverable2.position);
        assertEquals(recoverable1.plan.tempBlobId, recoverable2.plan.tempBlobId);
        assertEquals(recoverable1.plan.finalBlobId, recoverable2.plan.finalBlobId);
    }

    @Test
    public void testCommitRecoverableSerde() throws IOException {

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableWriter.CommitRecoverable commitRecoverable1 =
                outputStream.closeForCommit().getRecoverable();

        SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> serializer =
                recoverableWriter.getCommitRecoverableSerializer();
        byte[] bytes = serializer.serialize(commitRecoverable1);
        RecoverableWriter.CommitRecoverable commitRecoverable2 =
                serializer.deserialize(serializer.getVersion(), bytes);

        GSRecoverable recoverable1 = (GSRecoverable) commitRecoverable1;
        GSRecoverable recoverable2 = (GSRecoverable) commitRecoverable2;

        assertEquals(recoverable1.position, recoverable2.position);
        assertEquals(recoverable1.plan.tempBlobId, recoverable2.plan.tempBlobId);
        assertEquals(recoverable1.plan.finalBlobId, recoverable2.plan.finalBlobId);
    }
}
