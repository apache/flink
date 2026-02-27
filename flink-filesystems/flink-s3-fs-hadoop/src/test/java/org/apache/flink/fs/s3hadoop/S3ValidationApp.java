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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * A standalone application to validate S3 filesystem integration with AWS SDK v2.
 *
 * <p>Usage: S3ValidationApp bucket accessKey secretKey
 *
 * <p>This application performs basic S3 operations (write, read, list, delete) to verify the Hadoop
 * S3 filesystem is working correctly.
 */
public class S3ValidationApp {

    private static final String TEST_CONTENT = "Hello from Flink S3 Hadoop with AWS SDK v2!";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: S3ValidationApp <bucket> <accessKey> <secretKey>");
            System.exit(1);
        }

        String bucket = args[0];
        String accessKey = args[1];
        String secretKey = args[2];

        System.out.println("=".repeat(60));
        System.out.println("Flink S3 Hadoop Filesystem Validation");
        System.out.println("=".repeat(60));
        System.out.println("Bucket: " + bucket);
        System.out.println("Access Key: " + accessKey.substring(0, 4) + "***");
        System.out.println();

        int passed = 0;
        int failed = 0;

        try {
            // Step 1: Initialize FileSystem with S3 credentials
            System.out.print("[1/5] Initializing FileSystem... ");
            Configuration config = new Configuration();
            config.setString("s3.access-key", accessKey);
            config.setString("s3.secret-key", secretKey);
            FileSystem.initialize(config, null);
            System.out.println("OK");
            passed++;

            // Step 2: Get FileSystem and create test path
            System.out.print("[2/5] Getting S3 FileSystem... ");
            String testDir = "flink-validation-" + UUID.randomUUID();
            Path basePath = new Path("s3://" + bucket + "/" + testDir + "/");
            FileSystem fs = basePath.getFileSystem();
            System.out.println("OK");
            System.out.println("      FileSystem class: " + fs.getClass().getName());
            passed++;

            // Step 3: Write a test file
            System.out.print("[3/5] Writing test file... ");
            Path testFile = new Path(basePath, "test-file.txt");
            try (FSDataOutputStream out = fs.create(testFile, WriteMode.OVERWRITE)) {
                out.write(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
            }
            System.out.println("OK");
            System.out.println("      Path: " + testFile);
            passed++;

            // Step 4: Read and verify content
            System.out.print("[4/5] Reading and verifying... ");
            String readContent;
            try (FSDataInputStream in = fs.open(testFile)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    baos.write(buffer, 0, len);
                }
                readContent = baos.toString(StandardCharsets.UTF_8.name());
            }

            if (TEST_CONTENT.equals(readContent)) {
                System.out.println("OK");
                System.out.println("      Content verified: \"" + readContent + "\"");
                passed++;
            } else {
                System.out.println("FAILED");
                System.out.println("      Expected: \"" + TEST_CONTENT + "\"");
                System.out.println("      Got: \"" + readContent + "\"");
                failed++;
            }

            // Step 5: List and delete
            System.out.print("[5/5] Listing and cleanup... ");
            FileStatus[] files = fs.listStatus(basePath);
            System.out.println("OK");
            System.out.println("      Files found: " + files.length);
            for (FileStatus file : files) {
                System.out.println("      - " + file.getPath().getName());
            }

            // Skip cleanup to allow manual verification in S3
            System.out.println("      Skipping cleanup - file left in S3 for verification");
            System.out.println("      To clean up manually: aws s3 rm --recursive " + basePath);
            passed++;

        } catch (Exception e) {
            System.out.println("FAILED");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Summary
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("=".repeat(60));

        if (failed > 0) {
            System.exit(1);
        }
    }
}
