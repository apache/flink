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

package eu.stratosphere.test.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.LogUtils;

public abstract class AbstractTestBase {
	private static final int DEFAULT_NUM_TASK_MANAGER = 1;
	
	private static final int MINIMUM_HEAP_SIZE_MB = 192;
	
	private static final long MEMORY_SIZE = 80;

	private int numTaskManager = DEFAULT_NUM_TASK_MANAGER;
	
	protected final Configuration config;
	
	protected NepheleMiniCluster executor;
	
	private final List<File> tempFiles;
	
		
	public AbstractTestBase(Configuration config) {
		verifyJvmOptions();
		this.config = config;
		this.tempFiles = new ArrayList<File>();
		
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public int getNumTaskManager() { return numTaskManager; }

	public void setNumTaskManager(int numTaskManager) { this.numTaskManager = numTaskManager; }

	// --------------------------------------------------------------------------------------------
	//  Local Test Cluster Life Cycle
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void startCluster() throws Exception {
		this.executor = new NepheleMiniCluster();
		this.executor.setDefaultOverwriteFiles(true);
		this.executor.setLazyMemoryAllocation(true);
		this.executor.setMemorySize(MEMORY_SIZE);
		this.executor.setNumTaskManager(this.numTaskManager);
		this.executor.start();
	}

	@After
	public void stopCluster() throws Exception {
		try {
			if (this.executor != null) {
				this.executor.stop();
				this.executor = null;
				FileSystem.closeAll();
				System.gc();
			}
		} finally {
			deleteAllTempFiles();
		}
	}
	
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
		File f = new File(baseDir, fileName);
		
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
		return getResultReader(resultPath, false);
	}
	
	public BufferedReader[] getResultReader(String resultPath, boolean inOrderOfFiles) throws IOException {
		File[] files = getAllInvolvedFiles(resultPath);
		
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
		File[] files = getAllInvolvedFiles(resultPath);
		BufferedInputStream[] inStreams = new BufferedInputStream[files.length];
		for (int i = 0; i < files.length; i++) {
			inStreams[i] = new BufferedInputStream(new FileInputStream(files[i]));
		}
		return inStreams;
	}
	
	public void readAllResultLines(List<String> target, String resultPath) throws IOException {
		readAllResultLines(target, resultPath, false);
	}
	
	public void readAllResultLines(List<String> target, String resultPath, boolean inOrderOfFiles) throws IOException {
		for (BufferedReader reader : getResultReader(resultPath, inOrderOfFiles)) {
			String s = null;
			while ((s = reader.readLine()) != null) {
				target.add(s);
			}
		}
	}
	
	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);
		
		String[] result = (String[]) list.toArray(new String[list.size()]);
		Arrays.sort(result);
		
		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);
		
		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}
	
	public void compareResultsByLinesInMemoryWithStrictOrder(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, true);
		
		String[] result = (String[]) list.toArray(new String[list.size()]);
		
		String[] expected = expectedResultStr.split("\n");
		
		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}
	
	public void compareKeyValueParisWithDelta(String expectedLines, String resultPath, String delimiter, double maxDelta) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);
		
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
	
	private File[] getAllInvolvedFiles(String resultPath) {
		File result = asFile(resultPath);
		if (!result.exists()) {
			Assert.fail("Result file was not written");
		}
		if (result.isDirectory()) {
			return result.listFiles();
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
