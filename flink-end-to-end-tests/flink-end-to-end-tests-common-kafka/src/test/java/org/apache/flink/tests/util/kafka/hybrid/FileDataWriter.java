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

import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FileDataWriter implements SourceSplitDataWriter<String> {

    private static final Logger LOG = LoggerFactory.getLogger(FileDataWriter.class);

    private final BufferedWriter fileWriter;

    public FileDataWriter(Path outputFile) {
        checkNotNull(outputFile);
        try {
            this.fileWriter = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Cannot initialize BufferedWriter for {}", outputFile.toAbsolutePath());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeRecords(Collection<String> records) {
        try {
            for (String record : records) {
                fileWriter.write(record);
                fileWriter.newLine();
            }
            fileWriter.flush();
        } catch (IOException e) {
            LOG.error("Cannot write records");
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws IOException {
        fileWriter.close();
    }
}
