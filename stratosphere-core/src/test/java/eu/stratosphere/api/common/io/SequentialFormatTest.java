/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.api.common.io;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.common.io.BinaryInputFormat;
import eu.stratosphere.api.common.io.BinaryOutputFormat;
import eu.stratosphere.api.common.io.BlockInfo;
import eu.stratosphere.api.common.io.FormatUtil;
import eu.stratosphere.api.common.io.SerializedInputFormat;
import eu.stratosphere.api.common.io.SerializedOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.LogUtils;

/**
 * Tests {@link SerializedInputFormat} and {@link SerializedOutputFormat}.
 */
@RunWith(Parameterized.class)
public class SequentialFormatTest {

	public class InputSplitSorter implements Comparator<FileInputSplit> {
		@Override
		public int compare(FileInputSplit o1, FileInputSplit o2) {
			int pathOrder = o1.getPath().getName().compareTo(o2.getPath().getName());
			return pathOrder == 0 ? Long.signum(o1.getStart() - o2.getStart()) : pathOrder;
		}
	}

	private int numberOfTuples;

	private long blockSize;

	private int degreeOfParallelism;

	private BlockInfo info = new SerializedInputFormat<IOReadableWritable>().createBlockInfo();

	private int[] rawDataSizes;

	private File tempFile;

	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	/**
	 * Initializes SequentialFormatTest.
	 */
	public SequentialFormatTest(int numberOfTuples, long blockSize, int degreeOfParallelism) {
		this.numberOfTuples = numberOfTuples;
		this.blockSize = blockSize;
		this.degreeOfParallelism = degreeOfParallelism;
		this.rawDataSizes = new int[degreeOfParallelism];
	}

	/**
	 * Count how many bytes would be written if all records were directly serialized
	 */
	@Before
	public void calcRawDataSize() throws IOException {
		int recordIndex = 0;
		for (int fileIndex = 0; fileIndex < this.degreeOfParallelism; fileIndex++) {
			ByteCounter byteCounter = new ByteCounter();
			DataOutputStream out = new DataOutputStream(byteCounter);
			for (int fileCount = 0; fileCount < this.getNumberOfTuplesPerFile(fileIndex); fileCount++, recordIndex++)
				this.getRecord(recordIndex).write(out);
			this.rawDataSizes[fileIndex] = byteCounter.getLength();
		}
	}

	/**
	 * Checks if the expected input splits were created
	 */
	@Test
	public void checkInputSplits() throws IOException {
		FileInputSplit[] inputSplits = this.createInputFormat().createInputSplits(0);
		Arrays.sort(inputSplits, new InputSplitSorter());

		int splitIndex = 0;
		for (int fileIndex = 0; fileIndex < this.degreeOfParallelism; fileIndex++) {
			List<FileInputSplit> sameFileSplits = new ArrayList<FileInputSplit>();
			Path lastPath = inputSplits[splitIndex].getPath();
			for (; splitIndex < inputSplits.length; splitIndex++) {
				if (!inputSplits[splitIndex].getPath().equals(lastPath))
					break;
				sameFileSplits.add(inputSplits[splitIndex]);
			}

			Assert.assertEquals(this.getExpectedBlockCount(fileIndex), sameFileSplits.size());

			long lastBlockLength =
				this.rawDataSizes[fileIndex] % (this.blockSize - this.info.getInfoSize()) + this.info.getInfoSize();
			for (int index = 0; index < sameFileSplits.size(); index++) {
				Assert.assertEquals(this.blockSize * index, sameFileSplits.get(index).getStart());
				if (index < sameFileSplits.size() - 1)
					Assert.assertEquals(this.blockSize, sameFileSplits.get(index).getLength());
			}
			Assert.assertEquals(lastBlockLength, sameFileSplits.get(sameFileSplits.size() - 1).getLength());
		}
	}

	/**
	 * Tests if the expected sequence and amount of data can be read
	 */
	@Test
	public void checkRead() throws IOException {
		SerializedInputFormat<Record> input = this.createInputFormat();
		FileInputSplit[] inputSplits = input.createInputSplits(0);
		Arrays.sort(inputSplits, new InputSplitSorter());
		int readCount = 0;
		for (FileInputSplit inputSplit : inputSplits) {
			input.open(inputSplit);
			Record record = new Record();
			while (!input.reachedEnd())
				if (input.nextRecord(record)) {
					this.checkEquals(this.getRecord(readCount), record);
					readCount++;
				}
		}
		Assert.assertEquals(this.numberOfTuples, readCount);
	}

	/**
	 * Tests the statistics of the given format.
	 */
	@Test
	public void checkStatistics() {
		SerializedInputFormat<Record> input = this.createInputFormat();
		BaseStatistics statistics = input.getStatistics(null);
		Assert.assertEquals(this.numberOfTuples, statistics.getNumberOfRecords());
	}

	@After
	public void cleanup() {
		this.deleteRecursively(this.tempFile);
	}

	private void deleteRecursively(File file) {
		if (file.isDirectory())
			for (File subFile : file.listFiles())
				this.deleteRecursively(subFile);
		else
			file.delete();
	}

	/**
	 * Write out the tuples in a temporary file and return it.
	 */
	@Before
	public void writeTuples() throws IOException {
		this.tempFile = File.createTempFile("SerializedInputFormat", null);
		this.tempFile.deleteOnExit();
		Configuration configuration = new Configuration();
		configuration.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, this.blockSize);
		if (this.degreeOfParallelism == 1) {
			SerializedOutputFormat output =
				FormatUtil.openOutput(SerializedOutputFormat.class, this.tempFile.toURI().toString(),
					configuration);
			for (int index = 0; index < this.numberOfTuples; index++)
				output.writeRecord(this.getRecord(index));
			output.close();
		} else {
			this.tempFile.delete();
			this.tempFile.mkdir();
			int recordIndex = 0;
			for (int fileIndex = 0; fileIndex < this.degreeOfParallelism; fileIndex++) {
				SerializedOutputFormat output =
					FormatUtil.openOutput(SerializedOutputFormat.class, this.tempFile.toURI() +
						"/"
						+ (fileIndex + 1), configuration);
				for (int fileCount = 0; fileCount < this.getNumberOfTuplesPerFile(fileIndex); fileCount++, recordIndex++)
					output.writeRecord(this.getRecord(recordIndex));
				output.close();
			}
		}
	}

	private int getNumberOfTuplesPerFile(int fileIndex) {
		return this.numberOfTuples / this.degreeOfParallelism;
	}

	/**
	 * Tests if the length of the file matches the expected value.
	 */
	@Test
	public void checkLength() {
		File[] files = this.tempFile.isDirectory() ? this.tempFile.listFiles() : new File[] { this.tempFile };
		Arrays.sort(files);
		for (int fileIndex = 0; fileIndex < this.degreeOfParallelism; fileIndex++) {
			long lastBlockLength = this.rawDataSizes[fileIndex] % (this.blockSize - this.info.getInfoSize());
			long expectedLength =
				(this.getExpectedBlockCount(fileIndex) - 1) * this.blockSize + this.info.getInfoSize() +
					lastBlockLength;
			Assert.assertEquals(expectedLength, files[fileIndex].length());
		}
	}

	protected SerializedInputFormat<Record> createInputFormat() {
		Configuration configuration = new Configuration();
		configuration.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, this.blockSize);

		final SerializedInputFormat<Record> inputFormat = new SerializedInputFormat<Record>();
		inputFormat.setFilePath(this.tempFile.toURI().toString());
		
		inputFormat.configure(configuration);
		return inputFormat;
	}

	/**
	 * Returns the record to write at the given position
	 */
	protected Record getRecord(int index) {
		return new Record(new IntValue(index), new StringValue(String.valueOf(index)));
	}

	/**
	 * Checks if both records are equal
	 */
	private void checkEquals(Record expected, Record actual) {
		Assert.assertEquals(expected.getNumFields(), actual.getNumFields());
		Assert.assertEquals(expected.getField(0, IntValue.class), actual.getField(0, IntValue.class));
		Assert.assertEquals(expected.getField(1, StringValue.class), actual.getField(1, StringValue.class));
	}

	private int getExpectedBlockCount(int fileIndex) {
		int expectedBlockCount =
			(int) Math.ceil((double) this.rawDataSizes[fileIndex] / (this.blockSize - this.info.getInfoSize()));
		return expectedBlockCount;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		ArrayList<Object[]> params = new ArrayList<Object[]>();
		for (int dop = 1; dop <= 2; dop++) {
			// numberOfTuples, blockSize, dop
			params.add(new Object[] { 100, BinaryOutputFormat.NATIVE_BLOCK_SIZE, dop });
			params.add(new Object[] { 100, 1000, dop });
			params.add(new Object[] { 100, 1 << 20, dop });
			params.add(new Object[] { 10000, 1000, dop });
			params.add(new Object[] { 10000, 1 << 20, dop });
		}
		return params;
	}

	/**
	 * Counts the bytes that would be written.
	 * 
	 */
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
