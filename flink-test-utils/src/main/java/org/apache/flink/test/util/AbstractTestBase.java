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


package org.apache.flink.test.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public abstract class AbstractTestBase {
	protected static final int MINIMUM_HEAP_SIZE_MB = 192;
	
	protected static final long TASK_MANAGER_MEMORY_SIZE = 80;

	protected static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = 1;

	protected static final int DEFAULT_NUM_TASK_MANAGER = 1;

	protected final Configuration config;
	
	protected NepheleMiniCluster executor;
	
	private final List<File> tempFiles;

	protected int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;

	protected int numTaskManager = DEFAULT_NUM_TASK_MANAGER;

	public AbstractTestBase(Configuration config) {
		verifyJvmOptions();
		this.config = config;
		this.tempFiles = new ArrayList<File>();
	}

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}
	// --------------------------------------------------------------------------------------------
	//  Local Test Cluster Life Cycle
	// --------------------------------------------------------------------------------------------
	
	public void startCluster() throws Exception {
		this.executor = new NepheleMiniCluster();
		this.executor.setDefaultOverwriteFiles(true);
		this.executor.setLazyMemoryAllocation(true);
		this.executor.setMemorySize(TASK_MANAGER_MEMORY_SIZE);
		this.executor.setTaskManagerNumSlots(taskManagerNumSlots);
		this.executor.setNumTaskManager(this.numTaskManager);
		this.executor.start();
	}
	
	public void stopCluster() throws Exception {
		try {
			
			int numUnreleasedBCVars = 0;
			{
				TaskManager[] tms = executor.getTaskManagers();
				if (tms != null) {
					for (TaskManager tm : tms) {
						numUnreleasedBCVars += tm.getBroadcastVariableManager().getNumberOfVariablesWithReferences();
					}
				}
			}
			
			if (this.executor != null) {
				this.executor.stop();
				this.executor = null;
				FileSystem.closeAll();
				System.gc();
			}
			
			Assert.assertEquals("Not all broadcast variables were released.", 0, numUnreleasedBCVars);
		}
		finally {
			deleteAllTempFiles();
		}
		
		
	}

	//------------------
	// Accessors
	//------------------

	public int getTaskManagerNumSlots() { return taskManagerNumSlots; }

	public void setTaskManagerNumSlots(int taskManagerNumSlots) { this.taskManagerNumSlots = taskManagerNumSlots; }

	public int getNumTaskManager() { return numTaskManager; }

	public void setNumTaskManager(int numTaskManager) { this.numTaskManager = numTaskManager; }

	
	// --------------------------------------------------------------------------------------------
	//  Temporary File Utilities
	// --------------------------------------------------------------------------------------------
	
	public String getTempDirPath(String dirName) throws IOException {
		File f = createAndRegisterTempFile(dirName);
		return f.toURI().toString();
	}
	
	public String getTempFilePath(String fileName) throws IOException {
		File f = createAndRegisterTempFile(fileName);
		return f.toURI().toString();
	}
	
	public String createTempFile(String fileName, String contents) throws IOException {
		File f = createAndRegisterTempFile(fileName);
		Files.write(contents, f, Charsets.UTF_8);
		return f.toURI().toString();
	}
	
	public File createAndRegisterTempFile(String fileName) throws IOException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		File f = new File(baseDir, this.getClass().getName() + "-" + fileName);
		
		if (f.exists()) {
			deleteRecursively(f);
		}
		
		File parentToDelete = f;
		while (true) {
			File parent = parentToDelete.getParentFile();
			if (parent == null) {
				throw new IOException("Missed temp dir while traversing parents of a temp file.");
			}
			if (parent.equals(baseDir)) {
				break;
			}
			parentToDelete = parent;
		}
		
		Files.createParentDirs(f);
		this.tempFiles.add(parentToDelete);
		return f;
	}
	
	private void deleteAllTempFiles() throws IOException {
		for (File f : this.tempFiles) {
			if (f.exists()) {
				deleteRecursively(f);
			}
		}
	}
	
	private static void deleteRecursively (File f) throws IOException {
		if (f.isDirectory()) {
			FileUtils.deleteDirectory(f);
		} else {
			f.delete();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Result Checking
	// --------------------------------------------------------------------------------------------
	
	public BufferedReader[] getResultReader(String resultPath) throws IOException {
		return getResultReader(resultPath, new String[]{}, false);
	}
	
	public BufferedReader[] getResultReader(String resultPath, String[] excludePrefixes, boolean inOrderOfFiles) throws IOException {
		File[] files = getAllInvolvedFiles(resultPath, excludePrefixes);
		
		if (inOrderOfFiles) {
			// sort the files after their name (1, 2, 3, 4)...
			// we cannot sort by path, because strings sort by prefix
			Arrays.sort(files, new Comparator<File>() {

				@Override
				public int compare(File o1, File o2) {
					try {
						int f1 = Integer.parseInt(o1.getName());
						int f2 = Integer.parseInt(o2.getName());
						return f1 < f2 ? -1 : (f1 > f2 ? 1 : 0);
					}
					catch (NumberFormatException e) {
						throw new RuntimeException("The file names are no numbers and cannot be ordered: " + 
									o1.getName() + "/" + o2.getName());
					}
				}
			});
		}
		
		BufferedReader[] readers = new BufferedReader[files.length];
		for (int i = 0; i < files.length; i++) {
			readers[i] = new BufferedReader(new FileReader(files[i]));
		}
		return readers;
	}
	
	
	
	public BufferedInputStream[] getResultInputStream(String resultPath) throws IOException {
		return getResultInputStream(resultPath, new String[]{});
	}
	
	public BufferedInputStream[] getResultInputStream(String resultPath, String[] excludePrefixes) throws IOException {
		File[] files = getAllInvolvedFiles(resultPath, excludePrefixes);
		BufferedInputStream[] inStreams = new BufferedInputStream[files.length];
		for (int i = 0; i < files.length; i++) {
			inStreams[i] = new BufferedInputStream(new FileInputStream(files[i]));
		}
		return inStreams;
	}
	
	public void readAllResultLines(List<String> target, String resultPath) throws IOException {
		readAllResultLines(target, resultPath, new String[]{});
	}
	
	public void readAllResultLines(List<String> target, String resultPath, String[] excludePrefixes) throws IOException {
		readAllResultLines(target, resultPath, excludePrefixes, false);
	}
	
	public void readAllResultLines(List<String> target, String resultPath, String[] excludePrefixes, boolean inOrderOfFiles) throws IOException {
		for (BufferedReader reader : getResultReader(resultPath, excludePrefixes, inOrderOfFiles)) {
			String s = null;
			while ((s = reader.readLine()) != null) {
				target.add(s);
			}
		}
	}
	
	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		compareResultsByLinesInMemory(expectedResultStr, resultPath, new String[]{});
	}
	
	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath, String[] excludePrefixes) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, excludePrefixes, false);
		
		String[] result = (String[]) list.toArray(new String[list.size()]);
		Arrays.sort(result);
		
		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);
		
		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}
	public void compareResultsByLinesInMemoryWithStrictOrder(String expectedResultStr, String resultPath) throws Exception {
		compareResultsByLinesInMemoryWithStrictOrder(expectedResultStr, resultPath, new String[]{});
	}
	
	public void compareResultsByLinesInMemoryWithStrictOrder(String expectedResultStr, String resultPath, String[] excludePrefixes) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, excludePrefixes, true);
		
		String[] result = (String[]) list.toArray(new String[list.size()]);
		
		String[] expected = expectedResultStr.split("\n");
		
		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}
	
	public void compareKeyValueParisWithDelta(String expectedLines, String resultPath, String delimiter, double maxDelta) throws Exception {
		compareKeyValueParisWithDelta(expectedLines, resultPath, new String[]{}, delimiter, maxDelta);
	}
	
	public void compareKeyValueParisWithDelta(String expectedLines, String resultPath, String[] excludePrefixes, String delimiter, double maxDelta) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, excludePrefixes, false);
		
		String[] result = (String[]) list.toArray(new String[list.size()]);
		String[] expected = expectedLines.isEmpty() ? new String[0] : expectedLines.split("\n");
		
		Assert.assertEquals("Wrong number of result lines.", expected.length, result.length);
		
		Arrays.sort(result);
		Arrays.sort(expected);
		
		for (int i = 0; i < expected.length; i++) {
			String[] expectedFields = expected[i].split(delimiter);
			String[] resultFields = result[i].split(delimiter);
			
			double expectedPayLoad = Double.parseDouble(expectedFields[1]);
			double resultPayLoad = Double.parseDouble(resultFields[1]);
			
			Assert.assertTrue("Values differ by more than the permissible delta", Math.abs(expectedPayLoad - resultPayLoad) < maxDelta);
		}
	}
	
	public static <X> void compareResultCollections(List<X> expected, List<X> actual, Comparator<X> comparator) {
		Assert.assertEquals(expected.size(), actual.size());
		
		Collections.sort(expected, comparator);
		Collections.sort(actual, comparator);
		
		for (int i = 0; i < expected.size(); i++) {
			Assert.assertEquals(expected.get(i), actual.get(i));
		}
	}
	
	private File[] getAllInvolvedFiles(String resultPath, String[] excludePrefixes) {
		final String[] exPrefs = excludePrefixes;
		File result = asFile(resultPath);
		if (!result.exists()) {
			Assert.fail("Result file was not written");
		}
		if (result.isDirectory()) {
			return result.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					for(String p: exPrefs) {
						if(name.startsWith(p)) {
							return false;
						}
					}
					return true;
				}
			});
		} else {
			return new File[] { result };
		}
	}
	
	public File asFile(String path) {
		try {
			URI uri = new URI(path);
			if (uri.getScheme().equals("file")) {
				return new File(uri.getPath());
			} else {
				throw new IllegalArgumentException("This path does not denote a local file.");
			}
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("This path does not describe a valid local file URI.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Miscellaneous helper methods
	// --------------------------------------------------------------------------------------------
	
	protected static Collection<Object[]> toParameterList(Configuration ... testConfigs) {
		ArrayList<Object[]> configs = new ArrayList<Object[]>();
		for (Configuration testConfig : testConfigs) {
			Object[] c = { testConfig };
			configs.add(c);
		}
		return configs;
	}
	
	protected static Collection<Object[]> toParameterList(List<Configuration> testConfigs) {
		LinkedList<Object[]> configs = new LinkedList<Object[]>();
		for (Configuration testConfig : testConfigs) {
			Object[] c = { testConfig };
			configs.add(c);
		}
		return configs;
	}
	
	public static PrintStream getNullPrintStream() {
		return new PrintStream(new OutputStream() {
			@Override
			public void write(int b) throws IOException {}
		});
	}
}
