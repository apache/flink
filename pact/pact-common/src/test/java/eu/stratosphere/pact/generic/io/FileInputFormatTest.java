/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.testutils.TestFileUtils;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.io.FileInputFormat.FileBaseStatistics;

public class FileInputFormatTest { 

	@Test
	public void testGetStatisticsNonExistingFile() {
		try {
			final Configuration config = new Configuration();
			config.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/none/existing/directory/");
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.configure(config);
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertNull("The file statistics should be null.", stats);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsOneFileNoCachedVersion() {
		try {
			final long SIZE = 1024 * 500;
			final Configuration config = TestFileUtils.getConfigForFile(SIZE);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.configure(config);
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsMultipleFilesNoCachedVersion() {
		try {
			final long SIZE1 = 2077;
			final long SIZE2 = 31909;
			final long SIZE3 = 10;
			final long TOTAL = SIZE1 + SIZE2 + SIZE3;
			
			final Configuration config = TestFileUtils.getConfigForDir(SIZE1, SIZE2, SIZE3);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.configure(config);
			
			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsOneFileWithCachedVersion() {
		try {
			final long SIZE = 50873;
			final long FAKE_SIZE = 10065;
			
			final Configuration config = TestFileUtils.getConfigForFile(SIZE);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics outDatedFakeStats = new FileBaseStatistics(stats.getLastModificationTime()-1, FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, reGathered.getTotalInputSize());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testGetStatisticsMultipleFilesWithCachedVersion() {
		try {
			final long SIZE1 = 2077;
			final long SIZE2 = 31909;
			final long SIZE3 = 10;
			final long TOTAL = SIZE1 + SIZE2 + SIZE3;
			final long FAKE_SIZE = 10065;
			
			final Configuration config = TestFileUtils.getConfigForDir(SIZE1, SIZE2, SIZE3);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.configure(config);
			FileBaseStatistics outDatedFakeStats = new FileBaseStatistics(stats.getLastModificationTime()-1, FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, reGathered.getTotalInputSize());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	// ------------------------------------------------------------------------
	
	private class DummyFileInputFormat extends FileInputFormat<PactInteger> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean reachedEnd() throws IOException {
			return true;
		}

		@Override
		public boolean nextRecord(PactInteger record) throws IOException {
			return false;
		}
	}
}
