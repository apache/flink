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
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.StringTokenizer;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.test.util.filesystem.FilesystemProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProviderPool;

/**
 * @author Erik Nijkamp
 * @author Fabian Hueske
 */
public abstract class TestBase extends TestCase {
	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	private static final Log LOG = LogFactory.getLog(TestBase.class);

	protected static ClusterProvider cluster;

	protected final Configuration config;

	protected final String clusterConfig;

	public TestBase(Configuration config, String clusterConfig) {
		this.clusterConfig = clusterConfig;
		this.config = config;
		verifyJvmOptions();
	}

	public TestBase(Configuration testConfig) {
		this(testConfig, Constants.DEFAULT_TEST_CONFIG);
	}

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
		Assert.assertTrue("IPv4 stack required - set JVM option: -Djava.net.preferIPv4Stack=true", "true".equals(System
				.getProperty("java.net.preferIPv4Stack")));
	}

	@Before
	public void startCluster() throws Exception {
		LOG.info("######################### start - cluster config : " + clusterConfig + " #########################");
		cluster = ClusterProviderPool.getInstance(clusterConfig);
	}

	@After
	public void stopCluster() throws Exception {
		LOG.info("######################### stop - cluster config : " + clusterConfig + " #########################");
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
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		try {
			cluster.submitJobAndWait(jobGraph, getJarFilePath());
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
	 * 
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
			LOG.info("Expected: " + expectedResult);
			LOG.info("Computed: " + computedResult);
		}

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			
			if (LOG.isDebugEnabled())
				LOG.info("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			
			Assert.assertTrue("Computed and expected lines differ", comp.compare(expectedLine, computedLine) != 0);
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
