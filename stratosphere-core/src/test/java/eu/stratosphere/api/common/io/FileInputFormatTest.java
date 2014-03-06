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

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.api.common.io.FileInputFormat.FileBaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.testutils.TestFileUtils;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.LogUtils;

public class FileInputFormatTest { 

	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.ERROR);
	}
	
	@Test
	public void testGetStatisticsNonExistingFile() {
		try {
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath("file:///some/none/existing/directory/");
			format.configure(new Configuration());
			
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
			String tempFile = TestFileUtils.createTempFile(SIZE);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
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
			
			String tempDir = TestFileUtils.createTempFileDir(SIZE1, SIZE2, SIZE3);
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
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
			
			String tempFile = TestFileUtils.createTempFile(SIZE);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the expired modification time. the call should return new accurate stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			
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
			
			String tempDir = TestFileUtils.createTempFileDir(SIZE1, SIZE2, SIZE3);
			
			DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
			
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics newStats = format.getStatistics(stats);
			Assert.assertTrue("Statistics object was changed", newStats == stats);

			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics fakeStats = new FileBaseStatistics(stats.getLastModificationTime(), FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics latest = format.getStatistics(fakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", FAKE_SIZE, latest.getTotalInputSize());
			
			// insert fake stats with the correct modification time. the call should return the fake stats
			format = new DummyFileInputFormat();
			format.setFilePath(tempDir);
			format.configure(new Configuration());
			
			FileBaseStatistics outDatedFakeStats = new FileBaseStatistics(stats.getLastModificationTime()-1, FAKE_SIZE, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
			BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, reGathered.getTotalInputSize());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	// ------------------------------------------------------------------------
	
	private class DummyFileInputFormat extends FileInputFormat<IntValue> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean reachedEnd() throws IOException {
			return true;
		}

		@Override
		public IntValue nextRecord(IntValue record) throws IOException {
			return null;
		}
	}
}
