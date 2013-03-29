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

package eu.stratosphere.pact.common.io;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.testutils.TestConfigUtils;
import eu.stratosphere.pact.common.testutils.TestFileSystem;
import eu.stratosphere.pact.common.testutils.TestFileUtils;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.PactConfigConstants;


public class DelimitedInputFormatSamplingTest {

	private static final String TEST_DATA1 = 
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n" +
			"1234567890\n";
	
	private static final int DEFAULT_NUM_SAMPLES = 4;
	
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
		} catch (Throwable t) {
			Assert.fail("Could not load the global configuration.");
		}
	}
	
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
	
	// ========================================================================
	
	private static final class TestDelimitedInputFormat extends eu.stratosphere.pact.generic.io.DelimitedInputFormat<PactInteger> {
		@Override
		public boolean readRecord(PactInteger target, byte[] bytes, int offset, int numBytes) {
			throw new UnsupportedOperationException();
		}
	}
}
