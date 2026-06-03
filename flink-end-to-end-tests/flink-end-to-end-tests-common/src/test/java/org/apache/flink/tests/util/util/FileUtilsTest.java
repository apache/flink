/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util.util;

import org.apache.flink.test.util.FileUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileUtils}. */
@ExtendWith(TestLoggerExtension.class)
class FileUtilsTest {

    private static final List<String> ORIGINAL_LINES = List.of("line1", "line2", "line3");

    private Path testFile;

    @BeforeEach
    void setupFile(@TempDir Path tmpDir) throws IOException {
        Path path = Files.createTempFile(tmpDir, null, null);

        Files.write(path, ORIGINAL_LINES);

        testFile = path;
    }

    @Test
    void replaceSingleMatch() throws IOException {
        FileUtils.replace(testFile, Pattern.compile("line1"), matcher -> "removed");

        assertThat(Files.readAllLines(testFile))
                .containsExactly("removed", ORIGINAL_LINES.get(1), ORIGINAL_LINES.get(2));
    }

    @Test
    void replaceMultipleMatch() throws IOException {
        FileUtils.replace(testFile, Pattern.compile("line(.*)"), matcher -> matcher.group(1));

        assertThat(Files.readAllLines(testFile)).containsExactly("1", "2", "3");
    }

    @Test
    void replaceWithEmptyLine() throws IOException {
        FileUtils.replace(testFile, Pattern.compile("line2"), matcher -> "");

        assertThat(Files.readAllLines(testFile))
                .containsExactly(ORIGINAL_LINES.get(0), "", ORIGINAL_LINES.get(2));
    }
}
