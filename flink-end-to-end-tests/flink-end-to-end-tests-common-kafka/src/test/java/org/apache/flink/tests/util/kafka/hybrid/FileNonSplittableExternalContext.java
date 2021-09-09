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

package org.apache.flink.tests.util.kafka.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FileNonSplittableExternalContext implements ExternalContext<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileNonSplittableExternalContext.class);

    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    private final Collection<SourceSplitDataWriter<String>> fileWriters = new ArrayList<>();

    private final Path fileSourceDir;

    public FileNonSplittableExternalContext(Path fileSourceDir) {
        this.fileSourceDir = fileSourceDir;
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        return FileSource.forRecordStreamFormat(
                        new TextLineFormat(),
                        org.apache.flink.core.fs.Path.fromLocalFile(fileSourceDir.toFile()))
                .build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        FileDataWriter fileDataWriter =
                new FileDataWriter(fileSourceDir.resolve(UUID.randomUUID().toString()));
        fileWriters.add(fileDataWriter);
        return fileDataWriter;
    }

    @Override
    public Collection<String> generateTestData(int splitIndex, long seed) {
        Random random = new Random(seed);
        List<String> randomStringRecords = new ArrayList<>();
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            randomStringRecords.add(generateRandomString(stringLength, random));
        }
        return randomStringRecords;
    }

    private String generateRandomString(int length, Random random) {
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
        }
        return sb.toString();
    }

    @Override
    public void close() {
        fileWriters.forEach(
                writer -> {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Cannot close file writer", e);
                    }
                });
        fileWriters.clear();
    }

    @Override
    public String toString() {
        return "Non-splittable File context";
    }

    /** Factory of {@link FileNonSplittableExternalContext}. */
    public static class Factory implements ExternalContext.Factory<String> {

        private final Path fileSourceDir;

        public Factory(Path fileSourceDir) {
            this.fileSourceDir = fileSourceDir;
        }

        @Override
        public ExternalContext<String> createExternalContext() {
            return new FileNonSplittableExternalContext(fileSourceDir);
        }
    }
}
