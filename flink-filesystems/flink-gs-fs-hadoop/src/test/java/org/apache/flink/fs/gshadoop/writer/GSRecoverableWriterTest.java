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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests recoverable writer operations, i.e. write, commit, persist, restore, etc. */
public class GSRecoverableWriterTest extends GSRecoverableWriterTestBase {

    @Test
    public void testSupportResume() {
        assertTrue(recoverableWriter.supportsResume());
    }

    @Test
    public void testRequiresCleanupOfRecoverableState() {
        assertTrue(recoverableWriter.requiresCleanupOfRecoverableState());
    }

    @Test
    public void testTempObjectBucketAndName() throws IOException {
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        String tempBucketName = outputStream.plan.tempBlobId.getBucket();
        String tempObjectName = outputStream.plan.tempBlobId.getName();

        assertEquals(TEMP_BUCKET_NAME, tempBucketName);

        // should be like .inprogress/FINAL_OBJECT_NAME/000ec763-72d7-4359-9d84-119eb5425691, except
        // different uuid
        String expectedStartsWith =
                String.format("%s/%s/", recoverableOptions.uploadTempPrefix, FINAL_OBJECT_NAME);
        assertTrue(tempObjectName.startsWith(expectedStartsWith));

        // the remainder should be a UUID, make sure we can parse it, UUID.fromString throws an
        // exception if invalid format
        String uuidString = tempObjectName.substring(expectedStartsWith.length());
        UUID.fromString(uuidString);
    }

    @Test
    public void testSimpleWrite() throws IOException {
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes);
        outputStream.closeForCommit().commit();

        assertEquals(1, recoverableWriterHelper.blobs.size());

        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        assertArrayEquals(bytes, finalBlob.toByteArray());
    }

    @Test
    public void testCompoundWrite() throws IOException {
        byte[] bytes1 = new byte[1024];
        byte[] bytes2 = new byte[1024];
        RANDOM.nextBytes(bytes1);
        RANDOM.nextBytes(bytes2);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes1);
        outputStream.write(bytes2);
        outputStream.closeForCommit().commit();

        assertEquals(1, recoverableWriterHelper.blobs.size());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(bytes1);
        bytes.write(bytes2);

        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        assertArrayEquals(bytes.toByteArray(), finalBlob.toByteArray());
    }

    @Test
    public void testCompoundWriteWithRestore() throws IOException {
        byte[] bytes1 = new byte[1024];
        byte[] bytes2 = new byte[1024];
        RANDOM.nextBytes(bytes1);
        RANDOM.nextBytes(bytes2);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes1);
        RecoverableWriter.ResumeRecoverable recoverable = outputStream.persist();

        outputStream.write(bytes2);

        // assume some sort of failure here, such that we have to restore to the prior recovery
        // point
        // and write the bytes againt
        outputStream = (GSRecoverableFsDataOutputStream) recoverableWriter.recover(recoverable);
        outputStream.write(bytes2);
        outputStream.closeForCommit().commit();

        assertEquals(1, recoverableWriterHelper.blobs.size());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(bytes1);
        bytes.write(bytes2);

        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        assertArrayEquals(bytes.toByteArray(), finalBlob.toByteArray());
    }

    @Test
    public void testCleanup() throws IOException {
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes);
        RecoverableWriter.ResumeRecoverable recoverable = outputStream.persist();
        recoverableWriter.cleanupRecoverableState(recoverable);

        assertEquals(0, recoverableWriterHelper.blobs.size());
    }

    @Test(expected = ClosedChannelException.class)
    public void testWriteAfterClose() throws IOException {
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes);
        outputStream.close();
        outputStream.write(bytes);
    }

    @Test
    public void testWriteByteAndPosition() throws IOException {
        final int byteValue = 27;
        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(byteValue);
        assertEquals(1, outputStream.getPos());

        outputStream.closeForCommit().commit();
        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        assertArrayEquals(new byte[] {byteValue}, finalBlob.toByteArray());
    }

    @Test
    public void testWriteArrayAndPosition() throws IOException {
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes);
        assertEquals(bytes.length, outputStream.getPos());

        outputStream.closeForCommit().commit();
        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        assertArrayEquals(bytes, finalBlob.toByteArray());
    }

    @Test
    public void testWritePartialArrayAndPosition() throws IOException {
        final int start = 512;
        final int length = 256;
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes, start, length);
        assertEquals(length, outputStream.getPos());

        outputStream.closeForCommit().commit();
        MockGSBlob finalBlob = recoverableWriterHelper.blobs.get(outputStream.plan.finalBlobId);
        byte[] expectedBytes = Arrays.copyOfRange(bytes, start, start + length);
        assertArrayEquals(expectedBytes, finalBlob.toByteArray());
    }

    @Test(expected = IOException.class)
    public void testIncompleteWrite() throws IOException {
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);

        // configure the mock to only write 512 bytes even though 1024 was requested
        // WriteChannel can do this (write less than requested), and the
        // RecoverableFsDataOutputStream
        // interface doesn't support this, so we throw an exception in this case
        recoverableWriterHelper.maxWriteBytes = 512;

        GSRecoverableFsDataOutputStream outputStream =
                (GSRecoverableFsDataOutputStream) recoverableWriter.open(FINAL_PATH);
        outputStream.write(bytes);
    }
}
