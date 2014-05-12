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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.io.FileInputFormat.FileBaseStatistics;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
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
	
	// ---- Tests for .deflate ---------
	
	/**
	 * Create directory with files with .deflate extension and see if it creates a split
	 * for each file. Each split has to start from the beginning.
	 */
	@Test
	public void testFileInputSplit() {
		try {
			String tempFile = TestFileUtils.createTempFileDirExtension(".deflate", "some", "stupid", "meaningless", "files");
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(tempFile);
			format.configure(new Configuration());
			FileInputSplit[] splits = format.createInputSplits(2);
			Assert.assertEquals(4, splits.length);
			for(FileInputSplit split : splits) {
				Assert.assertEquals(-1L, split.getLength()); // unsplittable deflate files have this size as a flag for "read whole file"
				Assert.assertEquals(0L, split.getStart()); // always read from the beginning.
			}
			
			// test if this also works for "mixed" directories
			TestFileUtils.createTempFileInDirectory(tempFile.replace("file:", ""), "this creates a test file with a random extension (at least not .deflate)");
			
			final DummyFileInputFormat formatMixed = new DummyFileInputFormat();
			formatMixed.setFilePath(tempFile);
			formatMixed.configure(new Configuration());
			FileInputSplit[] splitsMixed = formatMixed.createInputSplits(2);
			Assert.assertEquals(5, splitsMixed.length);
			for(FileInputSplit split : splitsMixed) {
				if(split.getPath().getName().endsWith(".deflate")) {
					Assert.assertEquals(-1L, split.getLength()); // unsplittable deflate files have this size as a flag for "read whole file"
					Assert.assertEquals(0L, split.getStart()); // always read from the beginning.
				} else {
					Assert.assertEquals(0L, split.getStart());
					Assert.assertTrue("split size not correct", split.getLength() > 0);
				}
			}
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}
	
	@Test
	public void testIgnoredUnderscoreFiles() {
		try {
			final String contents = "CONTENTS";
			
			// create some accepted, some ignored files
			
			File tempDir = new File(System.getProperty("java.io.tmpdir"));
			File f = null;
			do {
				f = new File(tempDir, TestFileUtils.randomFileName(""));
			} while (f.exists());
			f.mkdirs();
			f.deleteOnExit();
			
			File child1 = new File(f, "dataFile1.txt");
			File child2 = new File(f, "another_file.bin");
			File luigiFile = new File(f, "_luigi");
			File success = new File(f, "_SUCCESS");
			
			File[] files = { child1, child2, luigiFile, success };
			
			for (File child : files) {
				child.deleteOnExit();
			
				BufferedWriter out = new BufferedWriter(new FileWriter(child));
				try { 
					out.write(contents);
				} finally {
					out.close();
				}
			}
			
			// test that only the valid files are accepted
			
			final DummyFileInputFormat format = new DummyFileInputFormat();
			format.setFilePath(f.toURI().toString());
			format.configure(new Configuration());
			FileInputSplit[] splits = format.createInputSplits(1);
			
			Assert.assertEquals(2, splits.length);
			
			final URI uri1 = splits[0].getPath().toUri();
			final URI uri2 = splits[1].getPath().toUri();

			final URI childUri1 = child1.toURI();
			final URI childUri2 = child2.toURI();
			
			Assert.assertTrue(  (uri1.equals(childUri1) && uri2.equals(childUri2)) ||
								(uri1.equals(childUri2) && uri2.equals(childUri1)) );
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
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
