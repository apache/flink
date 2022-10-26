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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link CsvOutputFormat}. */
class CsvOutputFormatTest {

    private String path = null;

    @BeforeEach
    void createFile() throws Exception {
        path = File.createTempFile("csv_output_test_file", ".csv").getAbsolutePath();
    }

    @Test
    void testNullAllow() throws Exception {
        final CsvOutputFormat<Tuple3<String, String, Integer>> csvOutputFormat =
                new CsvOutputFormat<>(new Path(path));
        try {
            csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
            csvOutputFormat.setAllowNullValues(true);
            csvOutputFormat.open(0, 1);
            csvOutputFormat.writeRecord(new Tuple3<>("One", null, 8));
        } finally {
            csvOutputFormat.close();
        }

        java.nio.file.Path p = Paths.get(path);
        assertThat(Files.exists(p)).isTrue();
        List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).isEqualTo("One,,8");
    }

    @Test
    void testNullDisallowOnDefault() throws Exception {
        final CsvOutputFormat<Tuple3<String, String, Integer>> csvOutputFormat =
                new CsvOutputFormat<>(new Path(path));
        try {
            csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
            csvOutputFormat.open(0, 1);
            try {
                csvOutputFormat.writeRecord(new Tuple3<>("One", null, 8));
                fail("should fail with an exception");
            } catch (RuntimeException e) {
                // expected
            }

        } finally {
            csvOutputFormat.close();
        }
    }

    @AfterEach
    void cleanUp() throws IOException {
        Files.deleteIfExists(Paths.get(path));
    }
}
