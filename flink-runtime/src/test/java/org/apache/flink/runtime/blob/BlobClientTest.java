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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.blob.BlobCachePutTest.verifyDeletedEventually;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/** This class contains unit tests for the {@link BlobClient}. */
class BlobClientTest {

    /** The buffer size used during the tests in bytes. */
    private static final int TEST_BUFFER_SIZE = 17 * 1000;

    /** The instance of the (non-ssl) BLOB server used during the tests. */
    static BlobServer blobServer;

    /** The blob service (non-ssl) client configuration. */
    static Configuration clientConfig;

    @TempDir static java.nio.file.Path tempDir;

    /** Starts the BLOB server. */
    @BeforeAll
    static void startServer() throws IOException {
        blobServer = TestingBlobUtils.createServer(tempDir);
        blobServer.start();

        clientConfig = new Configuration();
    }

    /** Shuts the BLOB server down. */
    @AfterAll
    static void stopServer() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }

    /**
     * Creates a test buffer and fills it with a specific byte pattern.
     *
     * @return a test buffer filled with a specific byte pattern
     */
    private static byte[] createTestBuffer() {
        final byte[] buf = new byte[TEST_BUFFER_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (i % 128);
        }
        return buf;
    }

    /**
     * Prepares a test file for the unit tests, i.e. the methods fills the file with a particular
     * byte patterns and computes the file's BLOB key.
     *
     * @param file the file to prepare for the unit tests
     * @return the BLOB key of the prepared file
     * @throws IOException thrown if an I/O error occurs while writing to the test file
     */
    private static byte[] prepareTestFile(File file) throws IOException {

        MessageDigest md = BlobUtils.createMessageDigest();

        final byte[] buf = new byte[TEST_BUFFER_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (i % 128);
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);

            for (int i = 0; i < 20; ++i) {
                fos.write(buf);
                md.update(buf);
            }

        } finally {
            if (fos != null) {
                fos.close();
            }
        }

        return md.digest();
    }

    /**
     * Validates the result of a GET operation by comparing the data from the retrieved input stream
     * to the content of the specified buffer.
     *
     * @param actualInputStream the input stream returned from the GET operation (will be closed by
     *     this method)
     * @param expectedBuf the buffer to compare the input stream's data to
     * @throws IOException thrown if an I/O error occurs while reading the input stream
     */
    static void validateGetAndClose(final InputStream actualInputStream, final byte[] expectedBuf)
            throws IOException {
        try {
            byte[] receivedBuffer = new byte[expectedBuf.length];

            int bytesReceived = 0;

            while (true) {

                final int read =
                        actualInputStream.read(
                                receivedBuffer,
                                bytesReceived,
                                receivedBuffer.length - bytesReceived);
                if (read < 0) {
                    throw new EOFException();
                }
                bytesReceived += read;

                if (bytesReceived == receivedBuffer.length) {
                    assertThat(actualInputStream.read()).isEqualTo(-1);
                    assertThat(receivedBuffer).isEqualTo(expectedBuf);
                    return;
                }
            }
        } finally {
            actualInputStream.close();
        }
    }

    /**
     * Validates the result of a GET operation by comparing the data from the retrieved input stream
     * to the content of the expected input stream.
     *
     * @param actualInputStream the input stream returned from the GET operation (will be closed by
     *     this method)
     * @param expectedInputStream the input stream to compare the input stream's data to
     * @throws IOException thrown if an I/O error occurs while reading any input stream
     */
    static void validateGetAndClose(InputStream actualInputStream, InputStream expectedInputStream)
            throws IOException {
        try {
            while (true) {
                final int r1 = actualInputStream.read();
                final int r2 = expectedInputStream.read();

                assertThat(r1).isEqualTo(r2);

                if (r1 < 0) {
                    break;
                }
            }
        } finally {
            actualInputStream.close();
            expectedInputStream.close();
        }
    }

    /**
     * Validates the result of a GET operation by comparing the data from the retrieved input stream
     * to the content of the specified file.
     *
     * @param actualInputStream the input stream returned from the GET operation
     * @param expectedFile the file to compare the input stream's data to
     * @throws IOException thrown if an I/O error occurs while reading the input stream or the file
     */
    @SuppressWarnings("WeakerAccess")
    static void validateGetAndClose(final InputStream actualInputStream, final File expectedFile)
            throws IOException {
        validateGetAndClose(actualInputStream, Files.newInputStream(expectedFile.toPath()));
    }

    protected boolean isSSLEnabled() {
        return false;
    }

    @Test
    void testContentAddressableBufferTransientBlob() throws IOException, InterruptedException {
        testContentAddressableBuffer(TRANSIENT_BLOB);
    }

    @Test
    void testContentAddressableBufferPermantBlob() throws IOException, InterruptedException {
        testContentAddressableBuffer(PERMANENT_BLOB);
    }

    /**
     * Tests the PUT/GET operations for content-addressable buffers.
     *
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testContentAddressableBuffer(BlobKey.BlobType blobType)
            throws IOException, InterruptedException {
        BlobClient client = null;

        try {
            byte[] testBuffer = createTestBuffer();
            MessageDigest md = BlobUtils.createMessageDigest();
            md.update(testBuffer);
            byte[] digest = md.digest();

            InetSocketAddress serverAddress =
                    new InetSocketAddress("localhost", getBlobServer().getPort());
            client = new BlobClient(serverAddress, getBlobClientConfig());

            JobID jobId = new JobID();

            // Store the data (job-unrelated)
            BlobKey receivedKey1 = null;
            if (blobType == TRANSIENT_BLOB) {
                receivedKey1 = client.putBuffer(null, testBuffer, 0, testBuffer.length, blobType);
                assertThat(receivedKey1.getHash()).isEqualTo(digest);
            }

            // try again with a job-related BLOB:
            BlobKey receivedKey2 =
                    client.putBuffer(jobId, testBuffer, 0, testBuffer.length, blobType);
            assertThat(receivedKey2.getHash()).isEqualTo(digest);
            if (blobType == TRANSIENT_BLOB) {
                verifyKeyDifferentHashEquals(receivedKey1, receivedKey2);
            }

            // Retrieve the data (job-unrelated)
            if (blobType == TRANSIENT_BLOB) {
                validateGetAndClose(client.getInternal(null, receivedKey1), testBuffer);
                // transient BLOBs should be deleted from the server, eventually
                verifyDeletedEventually(getBlobServer(), null, receivedKey1);
            }
            // job-related
            validateGetAndClose(client.getInternal(jobId, receivedKey2), testBuffer);
            if (blobType == TRANSIENT_BLOB) {
                // transient BLOBs should be deleted from the server, eventually
                verifyDeletedEventually(getBlobServer(), jobId, receivedKey2);
            }

            // Check reaction to invalid keys for job-unrelated blobs
            final BlobClient finalClient1 = client;
            assertThatThrownBy(() -> finalClient1.getInternal(null, BlobKey.createKey(blobType)))
                    .isInstanceOf(IOException.class);

            // Check reaction to invalid keys for job-related blobs
            // new client needed (closed from failure above)
            client = new BlobClient(serverAddress, getBlobClientConfig());
            final BlobClient finalClient2 = client;
            assertThatThrownBy(() -> finalClient2.getInternal(jobId, BlobKey.createKey(blobType)))
                    .isInstanceOf(IOException.class);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    protected Configuration getBlobClientConfig() {
        return clientConfig;
    }

    protected BlobServer getBlobServer() {
        return blobServer;
    }

    @Test
    void testContentAddressableStreamTransientBlob() throws IOException, InterruptedException {
        testContentAddressableStream(TRANSIENT_BLOB);
    }

    @Test
    void testContentAddressableStreamPermanentBlob() throws IOException, InterruptedException {
        testContentAddressableStream(PERMANENT_BLOB);
    }

    /**
     * Tests the PUT/GET operations for content-addressable streams.
     *
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testContentAddressableStream(BlobKey.BlobType blobType)
            throws IOException, InterruptedException {

        File testFile = tempDir.resolve("test_file").toFile();
        byte[] digest = prepareTestFile(testFile);

        InputStream is = null;

        try (BlobClient client =
                new BlobClient(
                        new InetSocketAddress("localhost", getBlobServer().getPort()),
                        getBlobClientConfig())) {

            JobID jobId = new JobID();
            BlobKey receivedKey1 = null;

            // Store the data (job-unrelated)
            if (blobType == TRANSIENT_BLOB) {
                is = Files.newInputStream(testFile.toPath());
                receivedKey1 = client.putInputStream(null, is, blobType);
                assertThat(receivedKey1.getHash()).isEqualTo(digest);
            }

            // try again with a job-related BLOB:
            is = Files.newInputStream(testFile.toPath());
            BlobKey receivedKey2 = client.putInputStream(jobId, is, blobType);

            is.close();
            is = null;

            // Retrieve the data (job-unrelated)
            if (blobType == TRANSIENT_BLOB) {
                verifyKeyDifferentHashEquals(receivedKey1, receivedKey2);

                validateGetAndClose(client.getInternal(null, receivedKey1), testFile);
                // transient BLOBs should be deleted from the server, eventually
                verifyDeletedEventually(getBlobServer(), null, receivedKey1);
            }
            // job-related
            validateGetAndClose(client.getInternal(jobId, receivedKey2), testFile);
            if (blobType == TRANSIENT_BLOB) {
                // transient BLOBs should be deleted from the server, eventually
                verifyDeletedEventually(getBlobServer(), jobId, receivedKey2);
            }
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Test
    void testGetFailsDuringStreamingNoJobTransientBlob() throws IOException {
        testGetFailsDuringStreaming(null, TRANSIENT_BLOB);
    }

    @Test
    void testGetFailsDuringStreamingForJobTransientBlob() throws IOException {
        testGetFailsDuringStreaming(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testGetFailsDuringStreamingForJobPermanentBlob() throws IOException {
        testGetFailsDuringStreaming(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Checks the correct result if a GET operation fails during the file download.
     *
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testGetFailsDuringStreaming(@Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {

        assumeThat(isSSLEnabled())
                .as("This test can deadlock when using SSL. See FLINK-19369.")
                .isFalse();

        try (BlobClient client =
                new BlobClient(
                        new InetSocketAddress("localhost", getBlobServer().getPort()),
                        getBlobClientConfig())) {

            byte[] data = new byte[5000000];
            Random rnd = new Random();
            rnd.nextBytes(data);

            // put content addressable (like libraries)
            BlobKey key = client.putBuffer(jobId, data, 0, data.length, blobType);
            assertThat(key).isNotNull();

            // issue a GET request that succeeds
            InputStream is = client.getInternal(jobId, key);

            byte[] receiveBuffer = new byte[data.length];
            int firstChunkLen = 50000;
            BlobUtils.readFully(is, receiveBuffer, 0, firstChunkLen, null);
            BlobUtils.readFully(is, receiveBuffer, firstChunkLen, firstChunkLen, null);

            // shut down the server
            for (BlobServerConnection conn : getBlobServer().getCurrentActiveConnections()) {
                conn.close();
            }

            try {
                BlobUtils.readFully(
                        is,
                        receiveBuffer,
                        2 * firstChunkLen,
                        data.length - 2 * firstChunkLen,
                        null);
                // we tolerate that this succeeds, as the receiver socket may have buffered
                // everything already, but in this case, also verify the contents
                assertThat(receiveBuffer).isEqualTo(data);
            } catch (IOException e) {
                // expected
            }
        }
    }

    /**
     * Tests the static {@link BlobClient#uploadFiles(InetSocketAddress, Configuration, JobID,
     * List)} helper.
     */
    @Test
    void testUploadJarFilesHelper() throws Exception {
        uploadJarFile(getBlobServer(), getBlobClientConfig());
    }

    /**
     * Tests the static {@link BlobClient#uploadFiles(InetSocketAddress, Configuration, JobID,
     * List)}} helper.
     */
    static void uploadJarFile(BlobServer blobServer, Configuration blobClientConfig)
            throws Exception {
        final File testFile = File.createTempFile("testfile", ".dat");
        testFile.deleteOnExit();
        prepareTestFile(testFile);

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());

        uploadJarFile(serverAddress, blobClientConfig, testFile);
        uploadJarFile(serverAddress, blobClientConfig, testFile);
    }

    private static void uploadJarFile(
            final InetSocketAddress serverAddress,
            final Configuration blobClientConfig,
            final File testFile)
            throws IOException {
        JobID jobId = new JobID();
        List<PermanentBlobKey> blobKeys =
                BlobClient.uploadFiles(
                        serverAddress,
                        blobClientConfig,
                        jobId,
                        Collections.singletonList(new Path(testFile.toURI())));

        assertThat(blobKeys).hasSize(1);

        try (BlobClient blobClient = new BlobClient(serverAddress, blobClientConfig)) {
            validateGetAndClose(blobClient.getInternal(jobId, blobKeys.get(0)), testFile);
        }
    }

    /** Tests the socket operation timeout. */
    @Test
    void testSocketTimeout() throws IOException {
        Configuration clientConfig = getBlobClientConfig();
        int oldSoTimeout = clientConfig.getInteger(BlobServerOptions.SO_TIMEOUT);

        clientConfig.setInteger(BlobServerOptions.SO_TIMEOUT, 50);

        try (final TestBlobServer testBlobServer =
                new TestBlobServer(
                        clientConfig,
                        tempDir.resolve("test_server").toFile(),
                        new VoidBlobStore(),
                        10_000L)) {
            testBlobServer.start();
            InetSocketAddress serverAddress =
                    new InetSocketAddress("localhost", testBlobServer.getPort());

            try (BlobClient client = new BlobClient(serverAddress, clientConfig)) {
                client.getInternal(new JobID(), BlobKey.createKey(TRANSIENT_BLOB));

                fail("Should throw an exception.");
            } catch (Throwable t) {
                assertThat(ExceptionUtils.findThrowable(t, java.net.SocketTimeoutException.class))
                        .isPresent();
            }
        } finally {
            clientConfig.setInteger(BlobServerOptions.SO_TIMEOUT, oldSoTimeout);
        }
    }

    @Test
    void testUnresolvedInetSocketAddress() throws Exception {
        try (BlobClient client =
                new BlobClient(
                        InetSocketAddress.createUnresolved("localhost", getBlobServer().getPort()),
                        getBlobClientConfig())) {
            assertThat(client.isConnected()).isTrue();
        }
    }

    static class TestBlobServer extends BlobServer {

        private final long blockingMillis;

        TestBlobServer(
                Configuration config,
                File storageDirectory,
                BlobStore blobStore,
                long blockingMillis)
                throws IOException {
            super(config, storageDirectory, blobStore);
            this.blockingMillis = blockingMillis;
        }

        @Override
        File getFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
            if (blockingMillis > 0) {
                try {
                    Thread.sleep(blockingMillis);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            return super.getFileInternal(jobId, blobKey);
        }
    }
}
