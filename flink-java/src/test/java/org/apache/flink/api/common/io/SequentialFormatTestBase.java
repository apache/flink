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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for {@link BinaryInputFormat} and {@link BinaryOutputFormat}. */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class SequentialFormatTestBase<T> {

    private static class InputSplitSorter implements Comparator<FileInputSplit> {
        @Override
        public int compare(FileInputSplit o1, FileInputSplit o2) {
            int pathOrder = o1.getPath().getName().compareTo(o2.getPath().getName());
            return pathOrder == 0 ? Long.signum(o1.getStart() - o2.getStart()) : pathOrder;
        }
    }

    @Parameter public int numberOfTuples;

    @Parameter(value = 1)
    public long blockSize;

    @Parameter(value = 2)
    public int parallelism;

    @Parameter(value = 3)
    public int[] rawDataSizes;

    protected File tempFile;

    /** Count how many bytes would be written if all records were directly serialized. */
    @BeforeEach
    void calcRawDataSize() throws IOException {
        int recordIndex = 0;
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            ByteCounter byteCounter = new ByteCounter();

            for (int fileCount = 0;
                    fileCount < this.getNumberOfTuplesPerFile(fileIndex);
                    fileCount++, recordIndex++) {
                writeRecord(
                        this.getRecord(recordIndex), new DataOutputViewStreamWrapper(byteCounter));
            }
            this.rawDataSizes[fileIndex] = byteCounter.getLength();
        }
    }

    /** Checks if the expected input splits were created. */
    @TestTemplate
    void checkInputSplits() throws IOException {
        FileInputSplit[] inputSplits = this.createInputFormat().createInputSplits(0);
        Arrays.sort(inputSplits, new InputSplitSorter());

        int splitIndex = 0;
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            List<FileInputSplit> sameFileSplits = new ArrayList<FileInputSplit>();
            Path lastPath = inputSplits[splitIndex].getPath();
            for (; splitIndex < inputSplits.length; splitIndex++) {
                if (!inputSplits[splitIndex].getPath().equals(lastPath)) {
                    break;
                }
                sameFileSplits.add(inputSplits[splitIndex]);
            }

            assertThat(this.getExpectedBlockCount(fileIndex)).isEqualTo(sameFileSplits.size());

            long lastBlockLength =
                    this.rawDataSizes[fileIndex] % (this.blockSize - getInfoSize()) + getInfoSize();
            for (int index = 0; index < sameFileSplits.size(); index++) {
                assertThat(this.blockSize * index).isEqualTo(sameFileSplits.get(index).getStart());
                if (index < sameFileSplits.size() - 1) {
                    assertThat(this.blockSize).isEqualTo(sameFileSplits.get(index).getLength());
                }
            }
            assertThat(lastBlockLength)
                    .isEqualTo(sameFileSplits.get(sameFileSplits.size() - 1).getLength());
        }
    }

    /** Tests if the expected sequence and amount of data can be read. */
    @TestTemplate
    void checkRead() throws Exception {
        BinaryInputFormat<T> input = this.createInputFormat();
        FileInputSplit[] inputSplits = input.createInputSplits(0);
        Arrays.sort(inputSplits, new InputSplitSorter());

        int readCount = 0;

        for (FileInputSplit inputSplit : inputSplits) {
            input.open(inputSplit);
            input.reopen(inputSplit, input.getCurrentState());

            T record = createInstance();

            while (!input.reachedEnd()) {
                if (input.nextRecord(record) != null) {
                    this.checkEquals(this.getRecord(readCount), record);

                    if (!input.reachedEnd()) {
                        Tuple2<Long, Long> state = input.getCurrentState();

                        input = this.createInputFormat();
                        input.reopen(inputSplit, state);
                    }
                    readCount++;
                }
            }
        }
        assertThat(this.numberOfTuples).isEqualTo(readCount);
    }

    /** Tests the statistics of the given format. */
    @TestTemplate
    void checkStatistics() {
        BinaryInputFormat<T> input = this.createInputFormat();
        BaseStatistics statistics = input.getStatistics(null);
        assertThat(this.numberOfTuples).isEqualTo(statistics.getNumberOfRecords());
    }

    @AfterEach
    void cleanup() {
        this.deleteRecursively(this.tempFile);
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File subFile : file.listFiles()) {
                this.deleteRecursively(subFile);
            }
        } else {
            file.delete();
        }
    }

    /** Write out the tuples in a temporary file and return it. */
    @BeforeEach
    void writeTuples() throws IOException {
        this.tempFile = File.createTempFile("BinaryInputFormat", null);
        this.tempFile.deleteOnExit();
        Configuration configuration = new Configuration();
        configuration.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, this.blockSize);
        if (this.parallelism == 1) {
            BinaryOutputFormat<T> output =
                    createOutputFormat(this.tempFile.toURI().toString(), configuration);
            for (int index = 0; index < this.numberOfTuples; index++) {
                output.writeRecord(this.getRecord(index));
            }
            output.close();
        } else {
            this.tempFile.delete();
            this.tempFile.mkdir();
            int recordIndex = 0;
            for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
                BinaryOutputFormat<T> output =
                        createOutputFormat(
                                this.tempFile.toURI() + "/" + (fileIndex + 1), configuration);
                for (int fileCount = 0;
                        fileCount < this.getNumberOfTuplesPerFile(fileIndex);
                        fileCount++, recordIndex++) {
                    output.writeRecord(this.getRecord(recordIndex));
                }
                output.close();
            }
        }
    }

    private int getNumberOfTuplesPerFile(int fileIndex) {
        return this.numberOfTuples / this.parallelism;
    }

    /** Tests if the length of the file matches the expected value. */
    @TestTemplate
    void checkLength() {
        File[] files =
                this.tempFile.isDirectory()
                        ? this.tempFile.listFiles()
                        : new File[] {this.tempFile};
        Arrays.sort(files);
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            long lastBlockLength = this.rawDataSizes[fileIndex] % (this.blockSize - getInfoSize());
            long expectedLength =
                    (this.getExpectedBlockCount(fileIndex) - 1) * this.blockSize
                            + getInfoSize()
                            + lastBlockLength;
            assertThat(expectedLength).isEqualTo(files[fileIndex].length());
        }
    }

    protected abstract BinaryInputFormat<T> createInputFormat();

    protected abstract BinaryOutputFormat<T> createOutputFormat(
            String path, Configuration configuration) throws IOException;

    protected abstract int getInfoSize();

    /** Returns the record to write at the given position. */
    protected abstract T getRecord(int index);

    protected abstract T createInstance();

    protected abstract void writeRecord(T record, DataOutputView outputView) throws IOException;

    /** Checks if both records are equal. */
    protected abstract void checkEquals(T expected, T actual);

    private int getExpectedBlockCount(int fileIndex) {
        int expectedBlockCount =
                (int)
                        Math.ceil(
                                (double) this.rawDataSizes[fileIndex]
                                        / (this.blockSize - getInfoSize()));
        return expectedBlockCount;
    }

    @Parameters
    public static List<Object[]> getParameters() {
        ArrayList<Object[]> params = new ArrayList<>();
        for (int parallelism = 1; parallelism <= 2; parallelism++) {
            // numberOfTuples, blockSize, parallelism
            params.add(
                    new Object[] {
                        100, BinaryOutputFormat.NATIVE_BLOCK_SIZE, parallelism, new int[parallelism]
                    });
            params.add(new Object[] {100, 1000, parallelism, new int[parallelism]});
            params.add(new Object[] {100, 1 << 20, parallelism, new int[parallelism]});
            params.add(new Object[] {10000, 1000, parallelism, new int[parallelism]});
            params.add(new Object[] {10000, 1 << 20, parallelism, new int[parallelism]});
        }
        return params;
    }

    /** Counts the bytes that would be written. */
    private static final class ByteCounter extends OutputStream {
        int length = 0;

        /**
         * Returns the length.
         *
         * @return the length
         */
        public int getLength() {
            return this.length;
        }

        @Override
        public void write(int b) throws IOException {
            this.length++;
        }
    }
}
