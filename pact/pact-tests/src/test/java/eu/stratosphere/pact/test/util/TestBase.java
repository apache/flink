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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.util.LogUtils;
import eu.stratosphere.pact.test.util.filesystem.FilesystemProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProviderPool;


public abstract class TestBase {
	
	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	private static final Log LOG = LogFactory.getLog(TestBase.class);

	protected static ClusterProvider cluster;

	protected final Configuration config;

	protected final String clusterConfig;

	public TestBase(Configuration config, String clusterConfig) {
		this.clusterConfig = clusterConfig;
		this.config = config;
		
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
		
		verifyJvmOptions();
	}

	public TestBase(Configuration testConfig) {
		this(testConfig, Constants.DEFAULT_TEST_CONFIG);
	}

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}

	@Before
	public void startCluster() throws Exception {
		cluster = ClusterProviderPool.getInstance(clusterConfig);
	}

	@After
	public void stopCluster() throws Exception {
		cluster.stopCluster();
		ClusterProviderPool.removeInstance(clusterConfig);
		FileSystem.closeAll();
		System.gc();
	}

	@Test
	public void testJob() throws Exception {
		// pre-submit
		preSubmit();

		// submit job
		JobGraph jobGraph = null;
		try {
			jobGraph = getJobGraph();
		} catch(Exception e) {
			LOG.error(e);
			e.printStackTrace();
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		try {
			final JobClient client = cluster.getJobClient(jobGraph, getJarFilePath());
			client.setConsoleStreamForReporting(TestBase2.getNullPrintStream());
			client.submitJobAndWait();
		} catch(Exception e) {
			LOG.error(e);
			Assert.fail("Job execution failed!");
		}
		
		// post-submit
		postSubmit();
	}

	/**
	 * Returns the FilesystemProvider of the cluster setup
	 * 
	 * @see eu.stratosphere.pact.test.util.filesystem.FilesystemProvider
	 * Assert.
	 * @return The FilesystemProvider of the cluster setup
	 */
	public FilesystemProvider getFilesystemProvider() {
		return cluster.getFilesystemProvider();
	}

	/**
	 * Helper method to ease the pain to construct valid JUnit test parameter
	 * lists
	 * 
	 * @param tConfigs
	 *            list of PACT test configurations
	 * @return list of JUnit test configurations
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static Collection<Object[]> toParameterList(Class<? extends TestBase> parent,
			List<Configuration> testConfigs) throws FileNotFoundException, IOException {
		String testClassName = parent.getName();

		File configDir = new File(Constants.TEST_CONFIGS);

		List<String> clusterConfigs = new ArrayList<String>();

		if (configDir.isDirectory()) {
			for (File configFile : configDir.listFiles()) {
				Properties p = new Properties();
				p.load(new FileInputStream(configFile));

				for (String key : p.stringPropertyNames()) {
					if (key.endsWith(testClassName)) {
						for (String config : p.getProperty(key).split(",")) {
							clusterConfigs.add(config);
						}
					}
				}
			}
		}

		if (clusterConfigs.isEmpty()) {
			LOG.warn("No test config defined for test-class '" + testClassName + "'. Using default config: '"+Constants.DEFAULT_TEST_CONFIG+"'.");	
			clusterConfigs.add(Constants.DEFAULT_TEST_CONFIG);
		}

		LinkedList<Object[]> configs = new LinkedList<Object[]>();
		for (String clusterConfig : clusterConfigs) {
			for (Configuration testConfig : testConfigs) {
				Object[] c = { clusterConfig, testConfig };
				configs.add(c);
			}
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

	/**
	 * Compares the expectedResultString and the file(s) in the HDFS linewise.
	 * Both results (expected and computed) are held in memory. Hence, this
	 * method should not be used to compare large results.
	 * 
	 * @param expectedResult
	 * @param hdfsPath
	 */
	protected void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {

		Comparator<String> defaultStrComp = new Comparator<String>() {
			@Override
			public int compare(String arg0, String arg1) {
				return arg0.compareTo(arg1);
			}
		};
		
		this.compareResultsByLinesInMemory(expectedResultStr, resultPath, defaultStrComp);
	}
	
	protected <T> void compareResultsByLinesInMemoryStrictOrder(List<T> result, String resultPath) throws Exception
	{
		final ArrayList<String> resultFiles = new ArrayList<String>();

		// Determine all result files
		if (getFilesystemProvider().isDir(resultPath)) {
			final String[] files = getFilesystemProvider().listFiles(resultPath);
			final Comparator<String> fileNameComp = new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					if (o1.length() < o2.length())
						return -1;
					else if (o1.length() > o2.length())
						return 1;
					else return o1.compareTo(o2);
				}
			};
			Arrays.sort(files, fileNameComp);
			
			for (String file : files) {
				if (!getFilesystemProvider().isDir(file)) {
					resultFiles.add(resultPath+"/"+file);
				}
			}
		} else {
			resultFiles.add(resultPath);
		}
		
		final Iterator<T> expectedLines = result.iterator();
		
		for (String resultFile : resultFiles) {
			// read each result file
			final InputStream is = getFilesystemProvider().getInputStream(resultFile);
			final BufferedReader reader = new BufferedReader(new InputStreamReader(is));

			// collect lines
			String line = null;
			while ((line = reader.readLine()) != null) {
				Assert.assertTrue("More lines in result than expected lines.", expectedLines.hasNext());
				String nextExpected = expectedLines.next().toString();
				Assert.assertEquals("Expected result and obtained result do not match.", nextExpected, line);
			}
			reader.close();
		}
		
		Assert.assertFalse("More expected lines than obtained lines.", expectedLines.hasNext());
	}
	
	/**
	 * Compares the expectedResultString and the file(s) in the HDFS linewise.
	 * Both results (expected and computed) are held in memory. Hence, this
	 * method should not be used to compare large results.
	 * 
	 * The line comparator is used to compare lines from the expected and result set.
	 * 
	 * @param expectedResult
	 * @param hdfsPath
	 * @param comp Line comparator
	 */
	protected void compareResultsByLinesInMemory(String expectedResultStr, String resultPath, Comparator<String> comp) throws Exception {

		ArrayList<String> resultFiles = new ArrayList<String>();

		// Determine all result files
		if (getFilesystemProvider().isDir(resultPath)) {
			for (String file : getFilesystemProvider().listFiles(resultPath)) {
				if (!getFilesystemProvider().isDir(file)) {
					resultFiles.add(resultPath+"/"+file);
				}
			}
		} else {
			resultFiles.add(resultPath);
		}

		// collect lines of all result files
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		for (String resultFile : resultFiles) {
			// read each result file
			InputStream is = getFilesystemProvider().getInputStream(resultFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line = reader.readLine();

			// collect lines
			while (line != null) {
				computedResult.add(line);
				line = reader.readLine();
			}
			reader.close();
		}

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(expectedResultStr, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// log expected and computed results
		if (LOG.isDebugEnabled()) {
			LOG.debug("Expected: " + expectedResult);
			LOG.debug("Computed: " + computedResult);
		}

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			
			if (LOG.isDebugEnabled())
				LOG.debug("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			
			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}
	}

	/**
	 * Returns the job which that has to be executed.
	 * 
	 * @return
	 * @throws Exception
	 */
	abstract protected JobGraph getJobGraph() throws Exception;

	/**
	 * Performs general-purpose pre-job submit logic, i.e. load job input into
	 * hdfs.
	 * 
	 * @return
	 * @throws Exception
	 */
	abstract protected void preSubmit() throws Exception;

	/**
	 * Performs general-purpose post-job submit logic, i.e. check
	 * 
	 * @return
	 * @throws Exception
	 */
	abstract protected void postSubmit() throws Exception;

	/**
	 * Returns the path to a JAR-file that contains all classes necessary to run
	 * the test This is only important if distributed tests are run!
	 * 
	 * @return Path to a JAR-file that contains all classes that are necessary
	 *         to run the test
	 */
	protected String getJarFilePath() {
		return null;
	}

}
