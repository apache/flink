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

import java.io.File;
import java.io.IOException;

import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.TestFileUtils;
import org.apache.flink.types.IntValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EnumerateNestedFilesTest { 

	protected Configuration config;
	final String tempPath = System.getProperty("java.io.tmpdir");

	private DummyFileInputFormat format;

	@Before
	public void setup() {
		this.config = new Configuration();
		format = new DummyFileInputFormat();
	}

	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
	}

	/**
	 * Test without nested directory and recursive.file.enumeration = true
	 */
	@Test
	public void testNoNestedDirectoryTrue() {
		try {
			String filePath = TestFileUtils.createTempFile("foo");

			this.format.setFilePath(new Path(filePath));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(1, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	/**
	 * Test with one nested directory and recursive.file.enumeration = true
	 */
	@Test
	public void testOneNestedDirectoryTrue() {
		try {
			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();

			// create a file in the first-level and two files in the nested dir
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), "paella");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "fideua");

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(3, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	/**
	 * Test with one nested directory and recursive.file.enumeration = false
	 */
	@Test
	public void testOneNestedDirectoryFalse() {
		try {
			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();

			// create a file in the first-level and two files in the nested dir
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), "paella");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "fideua");

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", false);
			format.configure(this.config);

			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(1, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	/**
	 * Test with two nested directories and recursive.file.enumeration = true
	 */
	@Test
	public void testTwoNestedDirectoriesTrue() {
		try {
			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();
			String thirdLevelDir = TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();

			File nestedNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir
					+ System.getProperty("file.separator") + thirdLevelDir);
			nestedNestedDir.mkdirs();
			nestedNestedDir.deleteOnExit();

			// create a file in the first-level, two files in the second level and one in the third level
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), "paella");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "fideua");
			TestFileUtils.createTempFileInDirectory(nestedNestedDir.getAbsolutePath(), "bravas");

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);
			
			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(4, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	/**
	 * Tests if the recursion is invoked correctly in nested directories.
	 */
	@Test
	public void testOnlyLevel2NestedDirectories() {
		try {
			String rootDir = TestFileUtils.randomFileName();
			String nestedDir = TestFileUtils.randomFileName();
			String firstNestedNestedDir = TestFileUtils.randomFileName();
			String secondNestedNestedDir = TestFileUtils.randomFileName();

			File testDir = new File(tempPath + System.getProperty("file.separator") + rootDir);
			testDir.mkdirs();
			testDir.deleteOnExit();

			File nested = new File(testDir.getAbsolutePath() + System.getProperty("file.separator") + nestedDir);
			nested.mkdirs();
			nested.deleteOnExit();

			File nestedNestedDir1 = new File(nested.getAbsolutePath() + System.getProperty("file.separator")
					+ firstNestedNestedDir);
			nestedNestedDir1.mkdirs();
			nestedNestedDir1.deleteOnExit();

			File nestedNestedDir2 = new File(nested.getAbsolutePath() + System.getProperty("file.separator")
					+ secondNestedNestedDir);
			nestedNestedDir2.mkdirs();
			nestedNestedDir2.deleteOnExit();

			// create files in second level
			TestFileUtils.createTempFileInDirectory(nestedNestedDir1.getAbsolutePath(), "paella");
			TestFileUtils.createTempFileInDirectory(nestedNestedDir1.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(nestedNestedDir2.getAbsolutePath(), "fideua");
			TestFileUtils.createTempFileInDirectory(nestedNestedDir2.getAbsolutePath(), "bravas");

			this.format.setFilePath(new Path(testDir.getAbsolutePath()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(4, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	/**
	 * Test with two nested directories and recursive.file.enumeration = true
	 */
	@Test
	public void testTwoNestedDirectoriesWithFilteredFilesTrue() {

		String sep = System.getProperty("file.separator");

		try {
			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();
			String thirdLevelDir = TestFileUtils.randomFileName();
			String secondLevelFilterDir = "_"+TestFileUtils.randomFileName();
			String thirdLevelFilterDir = "_"+TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + sep + firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + sep + firstLevelDir + sep + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();
			File insideNestedDirFiltered = new File(tempPath + sep + firstLevelDir + sep + secondLevelFilterDir);
			insideNestedDirFiltered.mkdirs();
			insideNestedDirFiltered.deleteOnExit();
			File filteredFile = new File(tempPath + sep + firstLevelDir + sep + "_IWillBeFiltered");
			filteredFile.createNewFile();
			filteredFile.deleteOnExit();

			File nestedNestedDir = new File(tempPath + sep + firstLevelDir + sep + secondLevelDir + sep + thirdLevelDir);
			nestedNestedDir.mkdirs();
			nestedNestedDir.deleteOnExit();
			File nestedNestedDirFiltered = new File(tempPath + sep + firstLevelDir + sep + secondLevelDir + sep + thirdLevelFilterDir);
			nestedNestedDirFiltered.mkdirs();
			nestedNestedDirFiltered.deleteOnExit();

			// create a file in the first-level, two files in the second level and one in the third level
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), "paella");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), "fideua");
			TestFileUtils.createTempFileInDirectory(nestedNestedDir.getAbsolutePath(), "bravas");
			// create files which are filtered
			TestFileUtils.createTempFileInDirectory(insideNestedDirFiltered.getAbsolutePath(), "kalamari");
			TestFileUtils.createTempFileInDirectory(insideNestedDirFiltered.getAbsolutePath(), "fideua");
			TestFileUtils.createTempFileInDirectory(nestedNestedDirFiltered.getAbsolutePath(), "bravas");

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			FileInputSplit[] splits = format.createInputSplits(1);
			Assert.assertEquals(4, splits.length);
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	@Test
	public void testGetStatisticsOneFileInNestedDir() {
		try {
			final long SIZE = 1024 * 500;
			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();

			// create a file in the nested dir
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), SIZE);

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", SIZE, stats.getTotalInputSize());
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}
	}

	@Test
	public void testGetStatisticsMultipleNestedFiles() {
		try {
			final long SIZE1 = 2077;
			final long SIZE2 = 31909;
			final long SIZE3 = 10;
			final long SIZE4 = 71;
			final long TOTAL = SIZE1 + SIZE2 + SIZE3 + SIZE4;

			String firstLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir = TestFileUtils.randomFileName();
			String secondLevelDir2 = TestFileUtils.randomFileName();

			File nestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir);
			nestedDir.mkdirs();
			nestedDir.deleteOnExit();

			File insideNestedDir = new File(tempPath + System.getProperty("file.separator") 
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir);
			insideNestedDir.mkdirs();
			insideNestedDir.deleteOnExit();

			File insideNestedDir2 = new File(tempPath + System.getProperty("file.separator")
					+ firstLevelDir + System.getProperty("file.separator") + secondLevelDir2);
			insideNestedDir2.mkdirs();
			insideNestedDir2.deleteOnExit();

			// create a file in the first-level and two files in the nested dir
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), SIZE1);
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), SIZE2);
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), SIZE3);
			TestFileUtils.createTempFileInDirectory(insideNestedDir2.getAbsolutePath(), SIZE4);

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());

			/* Now invalidate the cache and check again */
			Thread.sleep(1000); // accuracy of file modification times is rather low
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), 42L);

			BaseStatistics stats2 = format.getStatistics(stats);
			Assert.assertNotEquals(stats2, stats);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL + 42L, stats2.getTotalInputSize());

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
		public IntValue nextRecord(IntValue reuse) throws IOException {
			return null;
		}
	}
}
