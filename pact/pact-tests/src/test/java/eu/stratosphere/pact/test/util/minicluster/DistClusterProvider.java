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

package eu.stratosphere.pact.test.util.minicluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.pact.test.util.Constants;
import eu.stratosphere.pact.test.util.filesystem.ExternalDFSProvider;

public class DistClusterProvider extends ClusterProvider {

	// Config Data
	private String nepheleConfigDir;

	private String hdfsConfigDir;

	// internal data
	private String jobManagerHostName;

	public DistClusterProvider(Configuration config)
													throws Exception {

		super(config);

		// TODO read config and set parameters
		this.nepheleConfigDir = config.getString(
			"DistClusterProvider#nepheleConfigDir", "");
		if (nepheleConfigDir.equals("")) {
			throw new Exception("No nephele config dir was specified");
		}
		this.hdfsConfigDir = config.getString(
			"DistClusterProvider#hdfsConfigDir", "");
		if (hdfsConfigDir.equals("")) {
			throw new Exception("No hdfs config dir was specified");
		}

	}

	@Override
	protected void startFS() throws Exception {

		if (fsIsRunning()) {
			return;
		}

		if(config.getString(Constants.FILESYSTEM_TYPE, "").equals("external_hdfs")) {
			filesystemProvider = new ExternalDFSProvider(this.hdfsConfigDir);
		} else {
			throw new IllegalConfigurationException("Invalid file system type: "+config.getString(Constants.FILESYSTEM_TYPE, ""));
		}
		
		filesystemProvider.start();
		this.filesystemRunning = true;
	}

	@Override
	protected void startNephele() throws Exception {

		if (nepheleIsRunning()) {
			return;
		}

		File nepheleConfigDir = new File(this.nepheleConfigDir);

		// get Nephele JobManager hostname
		File nepheleMasterFile = new File(nepheleConfigDir.getAbsoluteFile() + "/master");
		if (nepheleMasterFile.exists()) {
			BufferedReader fr = new BufferedReader(new FileReader(nepheleMasterFile));
			this.jobManagerHostName = fr.readLine();
		} else {
			throw new Exception("Nephele Master File not found");
		}

		// start Nephele cluster

		File nepheleStartScript = new File(nepheleConfigDir.getAbsolutePath() + "/../bin/start-cluster.sh");
		try {
			Runtime.getRuntime().exec(nepheleStartScript.getAbsolutePath());
		} catch (IOException e1) {
			throw new Exception("Nephele Config File not found");
		}
		// wait 5s to get Nephele up
		Thread.sleep(5000);

		this.nepheleRunning = true;

	}

	@Override
	protected void stopFS() throws Exception {

		if (!fsIsRunning()) {
			return;
		}

		// stop HDFS provider
		filesystemProvider.stop();
		this.filesystemRunning = false;

	}

	@Override
	protected void stopNephele() throws Exception {

		if (!nepheleIsRunning()) {
			return;
		}

		// stop Nephele
		File nepheleConfigDir = new File(this.nepheleConfigDir);
		File nepheleStopScript = new File(nepheleConfigDir.getAbsolutePath() + "/../bin/stop-cluster.sh");
		Runtime.getRuntime().exec(nepheleStopScript.getAbsolutePath());

		this.nepheleRunning = false;

	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.minicluster.ClusterProvider#getJobClient(eu.stratosphere.nephele.jobgraph.JobGraph, java.lang.String)
	 */
	@Override
	public JobClient getJobClient(JobGraph jobGraph, String jarFilePath) throws Exception
	{
		if (jarFilePath == null) {
			throw new Exception("jar file path not specified");
		}
		
		final Path testJarFile = new Path(jarFilePath);
		jobGraph.addJar(testJarFile);
		
		// set up job configuration
		final Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, this.jobManagerHostName);

		return new JobClient(jobGraph, configuration);
	}

}
