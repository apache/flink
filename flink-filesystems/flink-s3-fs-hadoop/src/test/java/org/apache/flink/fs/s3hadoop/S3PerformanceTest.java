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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.testutils.s3.S3TestCredentials;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance integration tests for S3 filesystem with AWS SDK v2.
 *
 * <p>These tests measure throughput and latency for various S3 operations including:
 *
 * <ul>
 *   <li>Small file latency (1KB files)
 *   <li>Large file throughput (10MB, 50MB, 100MB)
 *   <li>Concurrent uploads and downloads
 *   <li>S3AccessHelper recovery operations (putObject, getObject, multipart upload)
 * </ul>
 *
 * <p>Tests are skipped if S3 credentials are not available in the environment.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3PerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(S3PerformanceTest.class);

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;

    private static FileSystem fs;
    private static Path basePath;
    private static S3AccessHelper s3AccessHelper;
    private static final Random random = new Random();

    @BeforeAll
    static void setup() throws IOException {
        S3TestCredentials.assumeCredentialsAvailable();

        Configuration config = new Configuration();
        config.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        config.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
        FileSystem.initialize(config);

        String testDir = "perf-test-" + UUID.randomUUID();
        basePath = new Path(S3TestCredentials.getTestBucketUri() + testDir + "/");
        fs = basePath.getFileSystem();

        // Get S3AccessHelper for testing recovery operations
        if (fs instanceof FlinkS3FileSystem) {
            FlinkS3FileSystem flinkS3Fs = (FlinkS3FileSystem) fs;
            org.apache.hadoop.fs.FileSystem hadoopFs = flinkS3Fs.getHadoopFileSystem();
            if (hadoopFs instanceof org.apache.hadoop.fs.s3a.S3AFileSystem) {
                org.apache.hadoop.fs.s3a.S3AFileSystem s3aFs =
                        (org.apache.hadoop.fs.s3a.S3AFileSystem) hadoopFs;
                s3AccessHelper = new HadoopS3AccessHelper(s3aFs, s3aFs.getConf());
            }
        }

        LOG.info("S3 Performance Test initialized with base path: {}", basePath);
    }

    @AfterAll
    static void teardown() throws IOException {
        if (fs != null && basePath != null) {
            LOG.info("Cleaning up test files at {}", basePath);
            fs.delete(basePath, true);
        }
    }

    @Test
    @Order(1)
    void testSmallFileLatency() throws Exception {
        LOG.info("Test 1: Small File Latency (1KB files)");

        int numFiles = 10;
        byte[] data = generateData(1 * KB);
        List<Long> writeTimes = new ArrayList<>();
        List<Long> readTimes = new ArrayList<>();
        List<Path> files = new ArrayList<>();

        // Write tests
        for (int i = 0; i < numFiles; i++) {
            Path file = new Path(basePath, "small-" + i + ".dat");
            files.add(file);

            long start = System.nanoTime();
            try (FSDataOutputStream out = fs.create(file, WriteMode.OVERWRITE)) {
                out.write(data);
            }
            writeTimes.add(System.nanoTime() - start);
        }

        // Read tests
        byte[] buffer = new byte[1 * KB];
        for (Path file : files) {
            long start = System.nanoTime();
            try (FSDataInputStream in = fs.open(file)) {
                int totalRead = 0;
                while (totalRead < buffer.length) {
                    int read = in.read(buffer, totalRead, buffer.length - totalRead);
                    if (read == -1) {
                        break;
                    }
                    totalRead += read;
                }
            }
            readTimes.add(System.nanoTime() - start);
        }

        // Cleanup
        for (Path file : files) {
            fs.delete(file, false);
        }

        LOG.info("  Write latency (avg): {:.2f} ms", average(writeTimes) / 1_000_000.0);
        LOG.info("  Write latency (min): {:.2f} ms", min(writeTimes) / 1_000_000.0);
        LOG.info("  Write latency (max): {:.2f} ms", max(writeTimes) / 1_000_000.0);
        LOG.info("  Read latency (avg):  {:.2f} ms", average(readTimes) / 1_000_000.0);
        LOG.info("  Read latency (min):  {:.2f} ms", min(readTimes) / 1_000_000.0);
        LOG.info("  Read latency (max):  {:.2f} ms", max(readTimes) / 1_000_000.0);
    }

    @Test
    @Order(2)
    void testLargeFileThroughput() throws Exception {
        LOG.info("Test 2: Large File Throughput");

        int[] sizes = {10 * MB, 50 * MB, 100 * MB};

        for (int size : sizes) {
            String sizeStr = (size / MB) + "MB";
            LOG.info("  Testing {} file:", sizeStr);

            byte[] data = generateData(size);
            Path file = new Path(basePath, "large-" + sizeStr + ".dat");

            // Write test
            long writeStart = System.nanoTime();
            try (FSDataOutputStream out = fs.create(file, WriteMode.OVERWRITE)) {
                int chunkSize = 8 * MB;
                int offset = 0;
                while (offset < data.length) {
                    int len = Math.min(chunkSize, data.length - offset);
                    out.write(data, offset, len);
                    offset += len;
                }
            }
            long writeElapsed = System.nanoTime() - writeStart;
            double writeThroughput = (size / (double) MB) / (writeElapsed / 1_000_000_000.0);

            // Read test
            long readStart = System.nanoTime();
            byte[] readBuffer = new byte[8 * MB];
            try (FSDataInputStream in = fs.open(file)) {
                while (in.read(readBuffer) != -1) {
                    // consume data
                }
            }
            long readElapsed = System.nanoTime() - readStart;
            double readThroughput = (size / (double) MB) / (readElapsed / 1_000_000_000.0);

            fs.delete(file, false);

            LOG.info(
                    "    Write: {:.2f} MB/s ({:.2f} s)",
                    writeThroughput,
                    writeElapsed / 1_000_000_000.0);
            LOG.info(
                    "    Read:  {:.2f} MB/s ({:.2f} s)",
                    readThroughput,
                    readElapsed / 1_000_000_000.0);
        }
    }

    @Test
    @Order(3)
    void testConcurrentUploads() throws Exception {
        LOG.info("Test 3: Concurrent Uploads (5MB files x 10 parallel)");

        int numFiles = 10;
        int fileSize = 5 * MB;
        byte[] data = generateData(fileSize);

        ExecutorService executor = Executors.newFixedThreadPool(numFiles);
        List<CompletableFuture<Long>> futures = new ArrayList<>();

        long overallStart = System.nanoTime();

        for (int i = 0; i < numFiles; i++) {
            final int idx = i;
            CompletableFuture<Long> future =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    Path file = new Path(basePath, "concurrent-" + idx + ".dat");
                                    long start = System.nanoTime();
                                    try (FSDataOutputStream out =
                                            fs.create(file, WriteMode.OVERWRITE)) {
                                        out.write(data);
                                    }
                                    return System.nanoTime() - start;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            executor);
            futures.add(future);
        }

        List<Long> times = new ArrayList<>();
        for (CompletableFuture<Long> f : futures) {
            times.add(f.get());
        }

        long overallElapsed = System.nanoTime() - overallStart;
        executor.shutdown();

        for (int i = 0; i < numFiles; i++) {
            fs.delete(new Path(basePath, "concurrent-" + i + ".dat"), false);
        }

        double totalDataMB = (numFiles * fileSize) / (double) MB;
        double overallThroughput = totalDataMB / (overallElapsed / 1_000_000_000.0);

        LOG.info("  Total data: {:.0f} MB", totalDataMB);
        LOG.info("  Wall time: {:.2f} s", overallElapsed / 1_000_000_000.0);
        LOG.info("  Aggregate throughput: {:.2f} MB/s", overallThroughput);
        LOG.info("  Avg per-file time: {:.2f} s", average(times) / 1_000_000_000.0);
    }

    @Test
    @Order(4)
    void testConcurrentDownloads() throws Exception {
        LOG.info("Test 4: Concurrent Downloads (5MB files x 10 parallel)");

        int numFiles = 10;
        int fileSize = 5 * MB;
        byte[] data = generateData(fileSize);

        // Upload test files
        LOG.info("  Uploading test files...");
        for (int i = 0; i < numFiles; i++) {
            Path file = new Path(basePath, "download-" + i + ".dat");
            try (FSDataOutputStream out = fs.create(file, WriteMode.OVERWRITE)) {
                out.write(data);
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(numFiles);
        List<CompletableFuture<Long>> futures = new ArrayList<>();

        long overallStart = System.nanoTime();

        for (int i = 0; i < numFiles; i++) {
            final int idx = i;
            CompletableFuture<Long> future =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    Path file = new Path(basePath, "download-" + idx + ".dat");
                                    byte[] buffer = new byte[1 * MB];
                                    long start = System.nanoTime();
                                    try (FSDataInputStream in = fs.open(file)) {
                                        while (in.read(buffer) != -1) {
                                            // consume data
                                        }
                                    }
                                    return System.nanoTime() - start;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            executor);
            futures.add(future);
        }

        List<Long> times = new ArrayList<>();
        for (CompletableFuture<Long> f : futures) {
            times.add(f.get());
        }

        long overallElapsed = System.nanoTime() - overallStart;
        executor.shutdown();

        for (int i = 0; i < numFiles; i++) {
            fs.delete(new Path(basePath, "download-" + i + ".dat"), false);
        }

        double totalDataMB = (numFiles * fileSize) / (double) MB;
        double overallThroughput = totalDataMB / (overallElapsed / 1_000_000_000.0);

        LOG.info("  Total data: {:.0f} MB", totalDataMB);
        LOG.info("  Wall time: {:.2f} s", overallElapsed / 1_000_000_000.0);
        LOG.info("  Aggregate throughput: {:.2f} MB/s", overallThroughput);
        LOG.info("  Avg per-file time: {:.2f} s", average(times) / 1_000_000_000.0);
    }

    @Test
    @Order(5)
    void testS3AccessHelperOperations() throws Exception {
        LOG.info("Test 5: S3AccessHelper Recovery Operations");

        if (s3AccessHelper == null) {
            LOG.info("  SKIPPED: S3AccessHelper not available");
            return;
        }

        String testPrefix = basePath.toUri().getPath().substring(1);
        int[] sizes = {1 * KB, 1 * MB, 5 * MB, 10 * MB};

        // Test putObject
        LOG.info("  putObject (direct S3 upload):");
        for (int size : sizes) {
            String sizeStr = size >= MB ? (size / MB) + "MB" : (size / KB) + "KB";
            File tempFile = createTempFile(size);
            String key = testPrefix + "put-" + sizeStr + ".dat";

            try {
                long start = System.nanoTime();
                s3AccessHelper.putObject(key, tempFile);
                long elapsed = System.nanoTime() - start;

                double throughput = (size / (double) MB) / (elapsed / 1_000_000_000.0);
                LOG.info(
                        "    {}: {:.2f} ms ({:.2f} MB/s)",
                        sizeStr,
                        elapsed / 1_000_000.0,
                        throughput);

                s3AccessHelper.deleteObject(key);
            } finally {
                tempFile.delete();
            }
        }

        // Test getObject
        LOG.info("  getObject (recovery download path):");
        for (int size : sizes) {
            String sizeStr = size >= MB ? (size / MB) + "MB" : (size / KB) + "KB";

            Path uploadPath = new Path(basePath, "get-" + sizeStr + ".dat");
            byte[] data = generateData(size);
            try (FSDataOutputStream out = fs.create(uploadPath, WriteMode.OVERWRITE)) {
                out.write(data);
            }

            String key = uploadPath.toUri().getPath().substring(1);
            File targetFile = File.createTempFile("s3-download-", ".dat");

            try {
                long start = System.nanoTime();
                long bytesRead = s3AccessHelper.getObject(key, targetFile);
                long elapsed = System.nanoTime() - start;

                double throughput = (bytesRead / (double) MB) / (elapsed / 1_000_000_000.0);
                LOG.info(
                        "    {}: {:.2f} ms ({:.2f} MB/s)",
                        sizeStr,
                        elapsed / 1_000_000.0,
                        throughput);

                fs.delete(uploadPath, false);
            } finally {
                targetFile.delete();
            }
        }

        // Test multipart upload
        LOG.info("  Multipart upload (start -> uploadPart -> commit):");
        int partSize = 5 * MB;
        int numParts = 3;
        int totalSize = partSize * numParts;
        String mpuKey = testPrefix + "multipart-test.dat";

        long mpuStart = System.nanoTime();

        String uploadId = s3AccessHelper.startMultiPartUpload(mpuKey);
        List<CompletedPart> completedParts = new ArrayList<>();

        for (int i = 1; i <= numParts; i++) {
            File partFile = createTempFile(partSize);
            try {
                var response = s3AccessHelper.uploadPart(mpuKey, uploadId, i, partFile, partSize);
                completedParts.add(
                        CompletedPart.builder().partNumber(i).eTag(response.eTag()).build());
            } finally {
                partFile.delete();
            }
        }

        s3AccessHelper.commitMultiPartUpload(
                mpuKey, uploadId, completedParts, totalSize, new AtomicInteger(0));

        long mpuElapsed = System.nanoTime() - mpuStart;
        double mpuThroughput = (totalSize / (double) MB) / (mpuElapsed / 1_000_000_000.0);

        LOG.info("    {} parts x {}MB = {}MB total", numParts, partSize / MB, totalSize / MB);
        LOG.info("    Total time: {:.2f} s", mpuElapsed / 1_000_000_000.0);
        LOG.info("    Throughput: {:.2f} MB/s", mpuThroughput);

        s3AccessHelper.deleteObject(mpuKey);
    }

    private static File createTempFile(int size) throws IOException {
        File tempFile = File.createTempFile("s3-perf-", ".dat");
        byte[] data = generateData(size);
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(data);
        }
        return tempFile;
    }

    private static byte[] generateData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    private static double average(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private static long min(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).min().orElse(0);
    }

    private static long max(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).max().orElse(0);
    }
}
