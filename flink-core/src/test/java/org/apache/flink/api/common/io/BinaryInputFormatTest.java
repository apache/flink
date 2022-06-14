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

package org.apache.flink.api.common.io;

import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.types.Record;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class BinaryInputFormatTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final class MyBinaryInputFormat extends BinaryInputFormat<Record> {

        private static final long serialVersionUID = 1L;

        @Override
        protected Record deserialize(Record record, DataInputView dataInput) {
            return record;
        }

        @Override
        public boolean supportsMultiPaths() {
            return true;
        }
    }

    @Test
    public void testCreateInputSplitsWithOneFile() throws IOException {
        final int blockInfoSize = new BlockInfo().getInfoSize();
        final int blockSize = blockInfoSize + 8;
        final int numBlocks = 3;

        // create temporary file with 3 blocks
        final File tempFile =
                createBinaryInputFile(
                        "test_create_input_splits_with_one_file", blockSize, numBlocks);

        final Configuration config = new Configuration();
        config.setLong("input.block_size", blockSize + 10);

        final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
        inputFormat.setFilePath(tempFile.toURI().toString());
        inputFormat.setBlockSize(blockSize);

        inputFormat.configure(config);

        FileInputSplit[] inputSplits = inputFormat.createInputSplits(numBlocks);

        assertThat(inputSplits).as("Returns requested numbers of splits.").hasSize(numBlocks);

        assertThat(inputSplits[0].getLength())
                .as("1. split should have block size length.")
                .isEqualTo(blockSize);

        assertThat(inputSplits[1].getLength())
                .as("2. split should have block size length.")
                .isEqualTo(blockSize);

        assertThat(inputSplits[2].getLength())
                .as("3. split should have block size length.")
                .isEqualTo(blockSize);
    }

    @Test
    public void testCreateInputSplitsWithMulitpleFiles() throws IOException {
        final int blockInfoSize = new BlockInfo().getInfoSize();
        final int blockSize = blockInfoSize + 8;
        final int numBlocks1 = 3;
        final int numBlocks2 = 5;

        final File tempFile1 =
                createBinaryInputFile("binary_input_format_test", blockSize, numBlocks1);
        final File tempFile2 =
                createBinaryInputFile("binary_input_format_test_2", blockSize, numBlocks2);
        final String pathFile1 = tempFile1.toURI().toString();
        final String pathFile2 = tempFile2.toURI().toString();

        final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
        inputFormat.setFilePaths(pathFile1, pathFile2);
        inputFormat.setBlockSize(blockSize);

        final int numBlocksTotal = numBlocks1 + numBlocks2;
        FileInputSplit[] inputSplits = inputFormat.createInputSplits(numBlocksTotal);

        int numSplitsFile1 = 0;
        int numSplitsFile2 = 0;

        assertThat(inputSplits).as("Returns requested numbers of splits.").hasSize(numBlocksTotal);

        for (int i = 0; i < inputSplits.length; i++) {

            assertThat(inputSplits[i].getLength())
                    .as("%d. split should have block size length.", i)
                    .isEqualTo(blockSize);

            if (inputSplits[i].getPath().toString().equals(pathFile1)) {
                numSplitsFile1++;
            } else if (inputSplits[i].getPath().toString().equals(pathFile2)) {
                numSplitsFile2++;
            } else {
                fail("Split does not belong to any input file.");
            }
        }
        assertThat(numSplitsFile1).isEqualTo(numBlocks1);
        assertThat(numSplitsFile2).isEqualTo(numBlocks2);
    }

    @Test
    public void testGetStatisticsNonExistingFiles() {
        final MyBinaryInputFormat format = new MyBinaryInputFormat();
        format.setFilePaths(
                "file:///some/none/existing/directory/",
                "file:///another/none/existing/directory/");
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats).as("The file statistics should be null.").isNull();
    }

    @Test
    public void testGetStatisticsMultiplePaths() throws IOException {
        final int blockInfoSize = new BlockInfo().getInfoSize();
        final int blockSize = blockInfoSize + 8;
        final int numBlocks1 = 3;
        final int numBlocks2 = 5;

        final File tempFile =
                createBinaryInputFile("binary_input_format_test", blockSize, numBlocks1);
        final File tempFile2 =
                createBinaryInputFile("binary_input_format_test_2", blockSize, numBlocks2);

        final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
        inputFormat.setFilePaths(tempFile.toURI().toString(), tempFile2.toURI().toString());
        inputFormat.setBlockSize(blockSize);

        BaseStatistics stats = inputFormat.getStatistics(null);

        assertThat(stats.getTotalInputSize())
                .as("The file size statistics is wrong")
                .isEqualTo(blockSize * (numBlocks1 + numBlocks2));
    }

    @Test
    public void testCreateInputSplitsWithEmptySplit() throws IOException {
        final int blockInfoSize = new BlockInfo().getInfoSize();
        final int blockSize = blockInfoSize + 8;
        final int numBlocks = 3;
        final int minNumSplits = 5;

        // create temporary file with 3 blocks
        final File tempFile =
                createBinaryInputFile(
                        "test_create_input_splits_with_empty_split", blockSize, numBlocks);

        final Configuration config = new Configuration();
        config.setLong("input.block_size", blockSize + 10);

        final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
        inputFormat.setFilePath(tempFile.toURI().toString());
        inputFormat.setBlockSize(blockSize);

        inputFormat.configure(config);

        FileInputSplit[] inputSplits = inputFormat.createInputSplits(minNumSplits);

        assertThat(inputSplits).as("Returns requested numbers of splits.").hasSize(minNumSplits);

        assertThat(inputSplits[0].getLength())
                .as("1. split should have block size length.")
                .isEqualTo(blockSize);

        assertThat(inputSplits[1].getLength())
                .as("2. split should have block size length.")
                .isEqualTo(blockSize);

        assertThat(inputSplits[2].getLength())
                .as("3. split should have block size length.")
                .isEqualTo(blockSize);

        assertThat(inputSplits[3].getLength()).as("4. split should be an empty split.").isZero();

        assertThat(inputSplits[4].getLength()).as("5. split should be an empty split.").isZero();
    }

    /** Creates a temp file with a certain number of blocks of a certain size. */
    private File createBinaryInputFile(String fileName, int blockSize, int numBlocks)
            throws IOException {
        final File tempFile = tempFolder.newFile(fileName);
        try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
            for (int i = 0; i < blockSize * numBlocks; i++) {
                fileOutputStream.write(new byte[] {1});
            }
        }
        return tempFile;
    }
}
