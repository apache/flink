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

package eu.stratosphere.pact.test.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;

public abstract class TestBase2 {
	
	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	protected final Configuration config;
	
	private final List<File> tempFiles;
	
	private NepheleMiniCluster executer;
	
	protected boolean printPlan = true;
	


	public TestBase2(Configuration config) {
		verifyJvmOptions();
		this.config = config;
		this.tempFiles = new ArrayList<File>();
	}
	

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}
	

	@Before
	public void startCluster() throws Exception {
		System.err.println("######################### STARTING LOCAL EXECUTION CONTEXT #########################");
		this.executer = new NepheleMiniCluster();
		this.executer.start();
	}

	@After
	public void stopCluster() throws Exception {
		try {
			if (this.executer != null) {
				System.err.println("######################### STOPPING LOCAL EXECUTION CONTEXT #########################");
				this.executer.stop();
				this.executer = null;
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
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Pre-submit work caused an error: " + e.getMessage());
		}

		// submit job
		JobGraph jobGraph = null;
		try {
			jobGraph = getJobGraph();
		} catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		Assert.assertNotNull("Obtained null JobGraph", jobGraph);
		
		try {
			JobClient client = this.executer.getJobClient(jobGraph);
			client.submitJobAndWait();
		} catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Job execution failed!");
		}
		
		// post-submit
		try {
			postSubmit();
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Post-submit work caused an error: " + e.getMessage());
		}
	}
	
	public String getTempDirPath(String dirName) throws IOException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		File f = new File(baseDir, dirName);
		return "file://" + f.getAbsolutePath();
	}
	
	public String getTempFilePath(String fileName) throws IOException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		File f = new File(baseDir, fileName);
		return "file://" + f.getAbsolutePath();
	}
	
	public String createTempFile(String fileName, String contents) throws IOException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		File f = new File(baseDir, fileName);
		
		if (f.exists()) {
			Files.deleteRecursively(f);
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
		Files.write(contents, f, Charsets.UTF_8);
		
		this.tempFiles.add(parentToDelete);
		return "file://" + f.getAbsolutePath();
	}
	
	public BufferedReader[] getResultReader(String resultPath) throws IOException {
		File[] files = getAllInvolvedFiles(resultPath);
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
		if (path.startsWith("file://")) {
			return new File(path.substring(7));
		} else {
			throw new IllegalArgumentException("This path does not denote a local file.");
		}
	}
	
	private void deleteAllTempFiles() throws IOException {
		for (File f : this.tempFiles) {
			if (f.exists()) {
				Files.deleteRecursively(f);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected JobGraph getJobGraph() throws Exception {
		Plan p = getPactPlan();
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
	
	protected Plan getPactPlan() {
		return null;
	}

	protected void preSubmit() throws Exception {}

	
	protected void postSubmit() throws Exception {}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to assess the correctness of the computed result
	// --------------------------------------------------------------------------------------------
	
//	/**
//	 * Compares the expectedResultString and the file(s) in the HDFS linewise.
//	 * Both results (expected and computed) are held in memory. Hence, this
//	 * method should not be used to compare large results.
//	 * 
//	 * @param expectedResult
//	 * @param hdfsPath
//	 */
//	protected void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
//
//		Comparator<String> defaultStrComp = new Comparator<String>() {
//			@Override
//			public int compare(String arg0, String arg1) {
//				return arg0.compareTo(arg1);
//			}
//		};
//		
//		this.compareResultsByLinesInMemory(expectedResultStr, resultPath, defaultStrComp);
//	}
//	
//	protected <T> void compareResultsByLinesInMemoryStrictOrder(List<T> result, String resultPath) throws Exception
//	{
//		final ArrayList<String> resultFiles = new ArrayList<String>();
//
//		// Determine all result files
//		if (getFilesystemProvider().isDir(resultPath)) {
//			final String[] files = getFilesystemProvider().listFiles(resultPath);
//			final Comparator<String> fileNameComp = new Comparator<String>() {
//				@Override
//				public int compare(String o1, String o2) {
//					if (o1.length() < o2.length())
//						return -1;
//					else if (o1.length() > o2.length())
//						return 1;
//					else return o1.compareTo(o2);
//				}
//			};
//			Arrays.sort(files, fileNameComp);
//			
//			for (String file : files) {
//				if (!getFilesystemProvider().isDir(file)) {
//					resultFiles.add(resultPath+"/"+file);
//				}
//			}
//		} else {
//			resultFiles.add(resultPath);
//		}
//		
//		final Iterator<T> expectedLines = result.iterator();
//		
//		for (String resultFile : resultFiles) {
//			// read each result file
//			final InputStream is = getFilesystemProvider().getInputStream(resultFile);
//			final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//
//			// collect lines
//			String line = null;
//			while ((line = reader.readLine()) != null) {
//				Assert.assertTrue("More lines in result than expected lines.", expectedLines.hasNext());
//				String nextExpected = expectedLines.next().toString();
//				Assert.assertEquals("Expected result and obtained result do not match.", nextExpected, line);
//			}
//			reader.close();
//		}
//		
//		Assert.assertFalse("More expected lines than obtained lines.", expectedLines.hasNext());
//	}
//	
//	/**
//	 * Compares the expectedResultString and the file(s) in the HDFS linewise.
//	 * Both results (expected and computed) are held in memory. Hence, this
//	 * method should not be used to compare large results.
//	 * 
//	 * The line comparator is used to compare lines from the expected and result set.
//	 * 
//	 * @param expectedResult
//	 * @param hdfsPath
//	 * @param comp Line comparator
//	 */
//	protected void compareResultsByLinesInMemory(String expectedResultStr, String resultPath, Comparator<String> comp) throws Exception {
//
//		ArrayList<String> resultFiles = new ArrayList<String>();
//
//		// Determine all result files
//		if (getFilesystemProvider().isDir(resultPath)) {
//			for (String file : getFilesystemProvider().listFiles(resultPath)) {
//				if (!getFilesystemProvider().isDir(file)) {
//					resultFiles.add(resultPath+"/"+file);
//				}
//			}
//		} else {
//			resultFiles.add(resultPath);
//		}
//
//		// collect lines of all result files
//		PriorityQueue<String> computedResult = new PriorityQueue<String>();
//		for (String resultFile : resultFiles) {
//			// read each result file
//			InputStream is = getFilesystemProvider().getInputStream(resultFile);
//			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//			String line = reader.readLine();
//
//			// collect lines
//			while (line != null) {
//				computedResult.add(line);
//				line = reader.readLine();
//			}
//			reader.close();
//		}
//
//		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
//		StringTokenizer st = new StringTokenizer(expectedResultStr, "\n");
//		while (st.hasMoreElements()) {
//			expectedResult.add(st.nextToken());
//		}
//
//		// log expected and computed results
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("Expected: " + expectedResult);
//			LOG.debug("Computed: " + computedResult);
//		}
//
//		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult.size());
//
//		while (!expectedResult.isEmpty()) {
//			String expectedLine = expectedResult.poll();
//			String computedLine = computedResult.poll();
//			
//			if (LOG.isDebugEnabled())
//				LOG.debug("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
//			
//			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
//		}
//	}
	
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
}
