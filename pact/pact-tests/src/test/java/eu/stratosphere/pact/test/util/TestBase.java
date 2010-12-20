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
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.StringTokenizer;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.test.util.minicluster.ClusterProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProviderPool;
import eu.stratosphere.pact.test.util.minicluster.HDFSProvider;

/**
 * @author Erik Nijkamp
 * @author Fabian Hueske
 */
public abstract class TestBase extends TestCase {
	private static final int MINIMUM_HEAP_SIZE_MB = 512;

	private static final Log LOG = LogFactory.getLog(TestBase.class);

	private static ClusterProvider cluster;

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
		cluster.getHDFSProvider().getFileSystem().close();
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
		JobGraph jobGraph = getJobGraph();
		cluster.submitJobAndWait(jobGraph, getJarFilePath());

		// post-submit
		postSubmit();
	}

	/**
	 * Returns the HDFS provider of the cluster setup
	 * 
	 * @return The HDFS provider of the cluster setup
	 */
	public HDFSProvider getHDFSProvider() {
		return cluster.getHDFSProvider();
	}

	/**
	 * Helper method to ease the pain to construct valid JUnit test parameter
	 * lists
	 * 
	 * @param tConfigs
	 *        list of PACT test configurations
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
					if (key.equals(testClassName)) {
						for (String config : p.getProperty(key).split(",")) {
							clusterConfigs.add(config);
						}
					}
				}
			}
		}

		if (clusterConfigs.isEmpty()) {
			LOG.warn("no test config defined for test-class '" + testClassName + "'");
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
	protected void compareResultsByLinesInMemory(String expectedResultStr, String hdfsPath) throws Exception {

		String[] resultFiles = new String[1];

		// Determine all result files
		if (getHDFSProvider().getFileSystem().getFileStatus(new Path(hdfsPath)).isDir()) {
			LinkedList<String> files = new LinkedList<String>();
			for (FileStatus fs : getHDFSProvider().getFileSystem().listStatus(new Path(hdfsPath))) {
				if (!fs.isDir()) {
					files.add(fs.getPath().toString());
				}
			}
			resultFiles = files.toArray(resultFiles);
		} else {
			resultFiles[0] = hdfsPath;
		}

		// collect lines of all result files
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		for (String resultFile : resultFiles) {
			// read each result file
			InputStream is = getHDFSProvider().getHdfsInputStream(resultFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line = reader.readLine();

			// collect lines
			while (line != null) {
				computedResult.add(line);
				line = reader.readLine();
			}
			reader.close();
		}
		assertEquals("Computed Result is empty", 0, computedResult.size());

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(expectedResultStr, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// log expected and computed results
		LOG.debug("Expected: " + expectedResult);
		LOG.debug("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
			.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
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
