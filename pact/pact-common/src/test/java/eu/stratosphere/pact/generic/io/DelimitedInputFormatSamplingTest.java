/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.generic.io;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.testutils.TestConfigUtils;
import eu.stratosphere.pact.common.testutils.TestFileSystem;
import eu.stratosphere.pact.common.testutils.TestFileUtils;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.PactConfigConstants;

public class DelimitedInputFormatSamplingTest {
	
	private static final String TEST_DATA1 = 
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n" +
			"123456789\n";
	
	private static final String TEST_DATA2 = 
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n" +
			"12345\n";
			
	private static final int TEST_DATA_1_LINES = TEST_DATA1.split("\n").length;
	
	private static final int TEST_DATA_1_LINEWIDTH = TEST_DATA1.split("\n")[0].length();
	
	private static final int TEST_DATA_2_LINEWIDTH = TEST_DATA2.split("\n")[0].length();
	
	private static final int TOTAL_SIZE = TEST_DATA1.length() + TEST_DATA2.length();
	
	private static final int DEFAULT_NUM_SAMPLES = 4;
	
	// ========================================================================
	//  Setup
	// ========================================================================
	
	@BeforeClass
	public static void initialize() {
		try {
			TestFileSystem.registerTestFileSysten();
		} catch (Throwable t) {
			Assert.fail("Could not setup the mock test filesystem.");
		}
		
		try {
			// make sure we do 4 samples
			TestConfigUtils.loadGlobalConf(
				new String[] { PactConfigConstants.DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY,
								PactConfigConstants.DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY },
				new String[] { "4", "4" });
			
			TestDelimitedInputFormat.prepare();
		} catch (Throwable t) {
			Assert.fail("Could not load the global configuration.");
		}
	}
	
	// ========================================================================
	//  Tests
	// ========================================================================
	
	@Test
	public void testNumSamplesOneFile() {
		try {
			final String tempFile = TestFileUtils.createTempFile(TEST_DATA1);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "test://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			format.getStatistics(null);
			Assert.assertEquals("Wrong number of samples taken.", DEFAULT_NUM_SAMPLES, TestFileSystem.getNumtimeStreamOpened());
			
			conf.setString(TestDelimitedInputFormat.NUM_STATISTICS_SAMPLES, "8");
			final TestDelimitedInputFormat format2 = new TestDelimitedInputFormat();
			format2.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			format2.getStatistics(null);
			Assert.assertEquals("Wrong number of samples taken.", 8, TestFileSystem.getNumtimeStreamOpened());
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testNumSamplesMultipleFiles() {
		try {
			final String tempFile = TestFileUtils.createTempFileDir(TEST_DATA1, TEST_DATA1, TEST_DATA1, TEST_DATA1);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "test://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			format.getStatistics(null);
			Assert.assertEquals("Wrong number of samples taken.", DEFAULT_NUM_SAMPLES, TestFileSystem.getNumtimeStreamOpened());
			
			conf.setString(TestDelimitedInputFormat.NUM_STATISTICS_SAMPLES, "8");
			final TestDelimitedInputFormat format2 = new TestDelimitedInputFormat();
			format2.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			format2.getStatistics(null);
			Assert.assertEquals("Wrong number of samples taken.", 8, TestFileSystem.getNumtimeStreamOpened());
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testSamplingOneFile() {
		try {
			final String tempFile = TestFileUtils.createTempFile(TEST_DATA1);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			BaseStatistics stats = format.getStatistics(null);
			
			final int numLines = TEST_DATA_1_LINES;
			final float avgWidth = ((float) TEST_DATA1.length()) / TEST_DATA_1_LINES;
			Assert.assertTrue("Wrong record count.", stats.getNumberOfRecords() < numLines + 1 & stats.getNumberOfRecords() > numLines - 1);
			Assert.assertTrue("Wrong avg record size.", stats.getAverageRecordWidth() < avgWidth + 1 & stats.getAverageRecordWidth() > avgWidth - 1);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testSamplingDirectory() {
		try {
			final String tempFile = TestFileUtils.createTempFileDir(TEST_DATA1, TEST_DATA2);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			BaseStatistics stats = format.getStatistics(null);
			
			final int maxNumLines = (int) Math.ceil(TOTAL_SIZE / ((double) Math.min(TEST_DATA_1_LINEWIDTH, TEST_DATA_2_LINEWIDTH)));
			final int minNumLines = (int) (TOTAL_SIZE / ((double) Math.max(TEST_DATA_1_LINEWIDTH, TEST_DATA_2_LINEWIDTH)));
			final float maxAvgWidth = ((float) (TOTAL_SIZE)) / minNumLines;
			final float minAvgWidth = ((float) (TOTAL_SIZE)) / maxNumLines;
			
			if (!(stats.getNumberOfRecords() <= maxNumLines  & stats.getNumberOfRecords() >= minNumLines)) {
				System.err.println("Records: " + stats.getNumberOfRecords() + " out of (" + minNumLines + ", " + maxNumLines + ").");
				Assert.fail("Wrong record count.");
			}
			if (!(stats.getAverageRecordWidth() <= maxAvgWidth & stats.getAverageRecordWidth() >= minAvgWidth)) {
				Assert.fail("Wrong avg record size.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testDifferentDelimiter() {
		try {
			final String DELIMITER = "12345678-";
			String testData = TEST_DATA1.replace("\n", DELIMITER);
			
			final String tempFile = TestFileUtils.createTempFile(testData);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://" + tempFile);
			conf.setString(TestDelimitedInputFormat.RECORD_DELIMITER, DELIMITER);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			
			BaseStatistics stats = format.getStatistics(null);
			final int numLines = TEST_DATA_1_LINES;
			final float avgWidth = ((float) testData.length()) / TEST_DATA_1_LINES;
			
			Assert.assertTrue("Wrong record count.", stats.getNumberOfRecords() < numLines + 1 & stats.getNumberOfRecords() > numLines - 1);
			Assert.assertTrue("Wrong avg record size.", stats.getAverageRecordWidth() < avgWidth + 1 & stats.getAverageRecordWidth() > avgWidth - 1);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testSamplingOverlyLongRecord() {
		try {
			final String tempFile = TestFileUtils.createTempFile(2 * PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			
			Assert.assertNull("Expected exception due to overly long record.", format.getStatistics(null));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testCachedStatistics() {
		try {
			final String tempFile = TestFileUtils.createTempFile(TEST_DATA1);
			final Configuration conf = new Configuration();
			conf.setString(FileInputFormat.FILE_PARAMETER_KEY, "test://" + tempFile);
			
			final TestDelimitedInputFormat format = new TestDelimitedInputFormat();
			format.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("Wrong number of samples taken.", DEFAULT_NUM_SAMPLES, TestFileSystem.getNumtimeStreamOpened());
			
			final TestDelimitedInputFormat format2 = new TestDelimitedInputFormat();
			format2.configure(conf);
			
			TestFileSystem.resetStreamOpenCounter();
			BaseStatistics stats2 = format2.getStatistics(stats);
			Assert.assertTrue("Using cached statistics should cicumvent sampling.", 0 == TestFileSystem.getNumtimeStreamOpened());
			Assert.assertTrue("Using cached statistics should cicumvent sampling.", stats == stats2);
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	// ========================================================================
	//  Mocks
	// ========================================================================
	
	private static final class TestDelimitedInputFormat extends DelimitedInputFormat<PactInteger> {
		@Override
		public boolean readRecord(PactInteger target, byte[] bytes, int offset, int numBytes) {
			throw new UnsupportedOperationException();
		}
		
		public static void prepare() {
			DelimitedInputFormat.loadGloablConfigParams();
		}
	}
}
