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
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.util.LogUtils;

public abstract class TestBase2 {
	
	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	protected final Configuration config;
	
	private final List<File> tempFiles;
	
	private NepheleMiniCluster executor;
	
	protected boolean printPlan = false;
	
	private JobExecutionResult jobExecutionResult;
	
	
	public TestBase2() {
		this(new Configuration());
	}
	
	public TestBase2(Configuration config) {
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
	

	@Before
	public void startCluster() throws Exception {
		this.executor = new NepheleMiniCluster();
		this.executor.setDefaultOverwriteFiles(true);
		
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

	@Test
	public void testJob() throws Exception {
		// pre-submit
		try {
			preSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Pre-submit work caused an error: " + e.getMessage());
		}
		
		// submit job
		JobGraph jobGraph = null;
		try {
			jobGraph = getJobGraph();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		Assert.assertNotNull("Obtained null JobGraph", jobGraph);
		
		try {
			JobClient client = this.executor.getJobClient(jobGraph);
			client.setConsoleStreamForReporting(getNullPrintStream());
			this.jobExecutionResult = client.submitJobAndWait();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Job execution failed!");
		}
		
		// post-submit
		try {
			postSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Post-submit work caused an error: " + e.getMessage());
		}
	}
	
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
	
	private File createAndRegisterTempFile(String fileName) throws IOException {
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
		
		String[] expected = expectedResultStr.split("\n");
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
	
	private void deleteAllTempFiles() throws IOException {
		for (File f : this.tempFiles) {
			if (f.exists()) {
				deleteRecursively(f);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected JobGraph getJobGraph() throws Exception {
		Plan p = getTestJob();
		if (p == null) {
			Assert.fail("Error: Cannot obtain Pact plan. Did the thest forget to override either 'getPactPlan()' or 'getJobGraph()' ?");
		}
		
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(p);
		
		if (printPlan) {
			System.out.println(new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op)); 
		}

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}
	
	protected Plan getTestJob() {
		return null;
	}

	protected void preSubmit() throws Exception {}

	
	protected void postSubmit() throws Exception {}
	
	public JobExecutionResult getJobExecutionResult() {
		return jobExecutionResult;
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
	
	private static void deleteRecursively (File f) throws IOException {
		if (f.isDirectory()) {
			FileUtils.deleteDirectory(f);
		} else {
			f.delete();
		}
	}
	
	public static PrintStream getNullPrintStream() {
		return new PrintStream(new OutputStream() {
			@Override
			public void write(int b) throws IOException {}
		});
	}
}
