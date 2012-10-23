/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.common.io;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Tests {@link SequentialInputFormat} and {@link SequentialOutputFormat}.
 * 
 * @author Arvid Heise
 */
@RunWith(Parameterized.class)
public class SequentialFormatTest {
	/**
	 * @author Arvid Heise
	 */
	public class InputSplitSorter implements Comparator<FileInputSplit> {
		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(FileInputSplit o1, FileInputSplit o2) {
			int pathOrder = o1.getPath().getName().compareTo(o2.getPath().getName());
			return pathOrder == 0 ? Long.signum(o1.getStart() - o2.getStart()) : pathOrder;
		}
	}

	private int numberOfTuples;

	private long blockSize;

	private int degreeOfParallelism;

	private BlockInfo info = new SequentialInputFormat().createBlockInfo();

	private int[] rawDataSizes;

	private File tempFile;

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
		for (int fileIndex = 0; fileIndex < degreeOfParallelism; fileIndex++) {
			ByteCounter byteCounter = new ByteCounter();
			DataOutputStream out = new DataOutputStream(byteCounter);
			for (int fileCount = 0; fileCount < getNumberOfTuplesPerFile(fileIndex); fileCount++, recordIndex++)
				getRecord(recordIndex).write(out);
			rawDataSizes[fileIndex] = byteCounter.getLength();
		}
	}

	/**
	 * Checks if the expected input splits were created
	 */
	@Test
	public void checkInputSplits() throws IOException {
		FileInputSplit[] inputSplits = createInputFormat().createInputSplits(0);
		Arrays.sort(inputSplits, new InputSplitSorter());

		int splitIndex = 0;
		for (int fileIndex = 0; fileIndex < degreeOfParallelism; fileIndex++) {
			List<FileInputSplit> sameFileSplits = new ArrayList<FileInputSplit>();
			Path lastPath = inputSplits[splitIndex].getPath();
			for (; splitIndex < inputSplits.length; splitIndex++)
				if (!inputSplits[splitIndex].getPath().equals(lastPath))
					break;
				else
					sameFileSplits.add(inputSplits[splitIndex]);

			Assert.assertEquals(getExpectedBlockCount(fileIndex), sameFileSplits.size());

			long lastBlockLength = rawDataSizes[fileIndex] % (blockSize - info.getInfoSize()) + info.getInfoSize();
			for (int index = 0; index < sameFileSplits.size(); index++) {
				Assert.assertEquals(blockSize * index, sameFileSplits.get(index).getStart());
				if (index < sameFileSplits.size() - 1)
					Assert.assertEquals(blockSize, sameFileSplits.get(index).getLength());
			}
			Assert.assertEquals(lastBlockLength, sameFileSplits.get(sameFileSplits.size() - 1).getLength());
		}
	}

	/**
	 * Tests if the expected sequence and amount of data can be read
	 */
	@Test
	public void checkRead() throws IOException {
		SequentialInputFormat input = createInputFormat();
		FileInputSplit[] inputSplits = input.createInputSplits(0);
		Arrays.sort(inputSplits, new InputSplitSorter());
		int readCount = 0;
		for (FileInputSplit inputSplit : inputSplits) {
			input.open(inputSplit);
			PactRecord record = new PactRecord();
			while (!input.reachedEnd())
				if (input.nextRecord(record)) {
					checkEquals(getRecord(readCount), record);
					readCount++;
				}
		}
		Assert.assertEquals(numberOfTuples, readCount);
	}

	/**
	 * Tests the statistics of the given format.
	 */
	@Test
	public void checkStatistics() throws IOException {
		SequentialInputFormat input = createInputFormat();
		BaseStatistics statistics = input.getStatistics(null);
		Assert.assertEquals(numberOfTuples, statistics.getNumberOfRecords());
	}

	@After
	public void cleanup() {
		deleteRecursively(tempFile);
	}

	private void deleteRecursively(File file) {
		if (file.isDirectory())
			for (File subFile : file.listFiles())
				deleteRecursively(subFile);
		else
			file.delete();
	}

	/**
	 * Write out the tuples in a temporary file and return it.
	 */
	@Before
	public void writeTuples() throws IOException {
		tempFile = File.createTempFile("SequentialInputFormat", null);
		tempFile.deleteOnExit();
		Configuration configuration = new Configuration();
		configuration.setLong(SequentialOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize);
		if (degreeOfParallelism == 1) {
			SequentialOutputFormat output =
				FormatUtil.openOutput(SequentialOutputFormat.class, "file://" + tempFile.getAbsolutePath(),
					configuration);
			for (int index = 0; index < numberOfTuples; index++)
				output.writeRecord(getRecord(index));
			output.close();
		} else {
			tempFile.delete();
			tempFile.mkdir();
			int recordIndex = 0;
			for (int fileIndex = 0; fileIndex < degreeOfParallelism; fileIndex++) {
				SequentialOutputFormat output =
					FormatUtil.openOutput(SequentialOutputFormat.class, "file://" + tempFile.getAbsolutePath() + "/"
						+ (fileIndex + 1), configuration);
				for (int fileCount = 0; fileCount < getNumberOfTuplesPerFile(fileIndex); fileCount++, recordIndex++)
					output.writeRecord(getRecord(recordIndex));
				output.close();
			}
		}
	}

	private int getNumberOfTuplesPerFile(int fileIndex) {
		return numberOfTuples / degreeOfParallelism;
	}

	/**
	 * Tests if the length of the file matches the expected value.
	 */
	@Test
	public void checkLength() throws IOException {
		File[] files = tempFile.isDirectory() ? tempFile.listFiles() : new File[] { tempFile };
		Arrays.sort(files);
		for (int fileIndex = 0; fileIndex < degreeOfParallelism; fileIndex++) {
			long lastBlockLength = rawDataSizes[fileIndex] % (blockSize - info.getInfoSize());
			long expectedLength =
				(getExpectedBlockCount(fileIndex) - 1) * blockSize + info.getInfoSize() + lastBlockLength;
			Assert.assertEquals(expectedLength, files[fileIndex].length());
		}
	}

	protected SequentialInputFormat createInputFormat() throws IOException {
		Configuration configuration = new Configuration();
		configuration.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://" + tempFile.getAbsolutePath());
		configuration.setLong(SequentialInputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize);

		final SequentialInputFormat inputFormat = new SequentialInputFormat();
		inputFormat.configure(configuration);
		return inputFormat;
	}

	/**
	 * Returns the record to write at the given position
	 */
	protected PactRecord getRecord(int index) {
		return new PactRecord(new PactInteger(index), new PactString(String.valueOf(index)));
	}

	/**
	 * Checks if both records are equal
	 */
	private void checkEquals(PactRecord expected, PactRecord actual) {
		Assert.assertEquals(expected.getNumFields(), actual.getNumFields());
		Assert.assertEquals(expected.getField(0, PactInteger.class), actual.getField(0, PactInteger.class));
		Assert.assertEquals(expected.getField(1, PactString.class), actual.getField(1, PactString.class));
	}

	private int getExpectedBlockCount(int fileIndex) {
		int expectedBlockCount = (int) Math.ceil((double) rawDataSizes[fileIndex] / (blockSize - info.getInfoSize()));
		return expectedBlockCount;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		ArrayList<Object[]> params = new ArrayList<Object[]>();
		for (int dop = 1; dop <= 2; dop++) {
			// numberOfTuples, blockSize, dop
			params.add(new Object[] { 100, SequentialOutputFormat.NATIVE_BLOCK_SIZE, dop });
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
	 * @author Arvid Heise
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
			length++;
		}
	}
}
