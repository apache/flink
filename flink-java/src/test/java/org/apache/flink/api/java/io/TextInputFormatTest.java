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

import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TextInputFormat}. */
public class TextInputFormatTest extends TestLogger {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSimpleRead() throws IOException {
        final String first = "First line";
        final String second = "Second line";

        // create input file
        File tempFile =
                File.createTempFile("TextInputFormatTest", "tmp", temporaryFolder.getRoot());
        tempFile.setWritable(true);

        try (PrintStream ps = new PrintStream(tempFile)) {
            ps.println(first);
            ps.println(second);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));

        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertThat("expected at least one input split", splits.length, greaterThanOrEqualTo(1));

        inputFormat.open(splits[0]);
        try {
            assertFalse(inputFormat.reachedEnd());
            String result = inputFormat.nextRecord("");
            assertNotNull("Expecting first record here", result);
            assertEquals(first, result);

            assertFalse(inputFormat.reachedEnd());
            result = inputFormat.nextRecord(result);
            assertNotNull("Expecting second record here", result);
            assertEquals(second, result);

            assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
        } finally {
            inputFormat.close();
        }
    }

    @Test
    public void testNestedFileRead() throws IOException {
        String[] dirs = new String[] {"first", "second"};
        List<String> expectedFiles = new ArrayList<>();

        File parentDir = temporaryFolder.getRoot();
        for (String dir : dirs) {
            // create input file
            File tmpDir = temporaryFolder.newFolder(dir);

            File tempFile = File.createTempFile("TextInputFormatTest", ".tmp", tmpDir);

            expectedFiles.add(
                    new Path(tempFile.getAbsolutePath())
                            .makeQualified(FileSystem.getLocalFileSystem())
                            .toString());
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(parentDir.toURI()));
        inputFormat.setNestedFileEnumeration(true);
        inputFormat.setNumLineSamples(10);

        // this is to check if the setter overrides the configuration (as expected)
        Configuration config = new Configuration();
        config.setBoolean("recursive.file.enumeration", false);
        config.setString("delimited-format.numSamples", "20");
        inputFormat.configure(config);

        assertTrue(inputFormat.getNestedFileEnumeration());
        assertEquals(10, inputFormat.getNumLineSamples());

        FileInputSplit[] splits = inputFormat.createInputSplits(expectedFiles.size());

        List<String> paths = new ArrayList<>();
        for (FileInputSplit split : splits) {
            paths.add(split.getPath().toString());
        }

        Collections.sort(expectedFiles);
        Collections.sort(paths);
        for (int i = 0; i < expectedFiles.size(); i++) {
            assertEquals(expectedFiles.get(i), paths.get(i));
        }
    }

    /**
     * This tests cases when line ends with \r\n and \n is used as delimiter, the last \r should be
     * removed.
     */
    @Test
    public void testRemovingTrailingCR() throws IOException {

        testRemovingTrailingCR("\n", "\n");
        testRemovingTrailingCR("\r\n", "\n");

        testRemovingTrailingCR("|", "|");
        testRemovingTrailingCR("|", "\n");
    }

    private void testRemovingTrailingCR(String lineBreaker, String delimiter) throws IOException {
        String first = "First line";
        String second = "Second line";
        String content = first + lineBreaker + second + lineBreaker;

        // create input file
        File tempFile =
                File.createTempFile("TextInputFormatTest", "tmp", temporaryFolder.getRoot());
        tempFile.setWritable(true);

        try (OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile))) {
            wrt.write(content);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
        inputFormat.setFilePath(tempFile.toURI().toString());

        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        inputFormat.setDelimiter(delimiter);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        String result;
        if ((delimiter.equals("\n") && (lineBreaker.equals("\n") || lineBreaker.equals("\r\n")))
                || (lineBreaker.equals(delimiter))) {

            result = inputFormat.nextRecord("");
            assertNotNull("Expecting first record here", result);
            assertEquals(first, result);

            result = inputFormat.nextRecord(result);
            assertNotNull("Expecting second record here", result);
            assertEquals(second, result);

            result = inputFormat.nextRecord(result);
            assertNull("The input file is over", result);

        } else {
            result = inputFormat.nextRecord("");
            assertNotNull("Expecting first record here", result);
            assertEquals(content, result);
        }
    }

    @Test
    public void testCompressedRead() throws IOException {
        TextInputFormat.registerInflaterInputStreamFactory(
                "compressed",
                new InflaterInputStreamFactory<InputStream>() {
                    @Override
                    public InputStream create(InputStream in) {
                        return in;
                    }

                    @Override
                    public Collection<String> getCommonFileExtensions() {
                        return Collections.singletonList("compressed");
                    }
                });

        final String first = "First line";
        final String second = "Second line";

        // create input file
        File tempFile =
                File.createTempFile(
                        "TextInputFormatTest", ".compressed", temporaryFolder.getRoot());
        tempFile.setWritable(true);

        try (PrintStream ps = new PrintStream(tempFile)) {
            ps.println(first);
            ps.println(second);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertThat("expected at least one input split", splits.length, greaterThanOrEqualTo(1));

        inputFormat.open(splits[0]);
        try {
            assertFalse(inputFormat.reachedEnd());
            String result = inputFormat.nextRecord("");
            assertNotNull("Expecting first record here", result);
            assertEquals(first, result);
            assertFalse(inputFormat.reachedEnd());

            Long currentOffset = inputFormat.getCurrentState();
            inputFormat.close();

            inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
            inputFormat.configure(parameters);
            inputFormat.reopen(splits[0], currentOffset);

            assertFalse(inputFormat.reachedEnd());
            result = inputFormat.nextRecord(result);
            assertNotNull("Expecting second record here", result);
            assertEquals(second, result);

            assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
        } finally {
            inputFormat.close();
        }
    }
}
