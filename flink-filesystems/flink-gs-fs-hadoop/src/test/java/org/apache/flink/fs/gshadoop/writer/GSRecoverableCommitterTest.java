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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test commit functionality. */
public class GSRecoverableCommitterTest extends GSRecoverableWriterTestBase {

    @Test(expected = RuntimeException.class)
    public void testDuplicateCommit() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableFsDataOutputStream.Committer committer = outputStream.closeForCommit();
        committer.commit();

        // try to commit again, this should fail
        committer.commit();
    }

    @Test
    public void testCommitAfterRecoveryWithNothingToDo() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableFsDataOutputStream.Committer committer = outputStream.closeForCommit();
        committer.commit();

        // this should succeed as it tolerates the temp file not being present
        committer.commitAfterRecovery();

        assertEquals(1, recoverableWriterHelper.blobs.size());
        assertNotNull(recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId));
    }

    @Test
    public void testCommitAfterRecoveryWithEverythingToDo() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableFsDataOutputStream.Committer committer = outputStream.closeForCommit();

        // this should succeed and perform the entire commit
        committer.commitAfterRecovery();

        assertEquals(1, recoverableWriterHelper.blobs.size());
        assertNotNull(recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId));
    }

    @Test
    public void testCommitAfterRecoveryWithDeleteTempToDo() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableFsDataOutputStream.Committer committer = outputStream.closeForCommit();
        committer.commit();

        // add back the temp blob, to simulate the situation where the copy has occurred but the
        // temp blob still needs to be deleted
        recoverableWriterHelper.getBlob(outputStream.plan.tempBlobId);

        // this should succeed, deleting the temp blob
        committer.commitAfterRecovery();

        // we should have just the final blob
        assertEquals(1, recoverableWriterHelper.blobs.size());
        assertNotNull(recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidCommitAfterRecovery() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        RecoverableFsDataOutputStream.Committer committer = outputStream.closeForCommit();
        committer.commit();

        // simulate the situation where both the temp and final blobs are missing, as would be the
        // case
        // if the temp blob upload timed out (i.e. took longer than a week)
        recoverableWriterHelper.blobs.remove(outputStream.plan.finalBlobId);
        assertEquals(0, recoverableWriterHelper.blobs.size());

        // this should fail, as there is no temp blob to copy but the final blob isn't in place
        committer.commitAfterRecovery();
    }
}
