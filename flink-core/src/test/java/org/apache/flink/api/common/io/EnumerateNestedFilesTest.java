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

	private final DummyFileInputFormat format = new DummyFileInputFormat();

	@Before
	public void setup() {
		this.config = new Configuration();
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
			final long TOTAL = SIZE1 + SIZE2 + SIZE3;

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
			TestFileUtils.createTempFileInDirectory(nestedDir.getAbsolutePath(), SIZE1);
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), SIZE2);
			TestFileUtils.createTempFileInDirectory(insideNestedDir.getAbsolutePath(), SIZE3);

			this.format.setFilePath(new Path(nestedDir.toURI().toString()));
			this.config.setBoolean("recursive.file.enumeration", true);
			format.configure(this.config);

			BaseStatistics stats = format.getStatistics(null);
			Assert.assertEquals("The file size from the statistics is wrong.", TOTAL, stats.getTotalInputSize());
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