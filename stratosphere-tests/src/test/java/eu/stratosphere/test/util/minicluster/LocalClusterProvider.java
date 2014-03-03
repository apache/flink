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

package eu.stratosphere.test.util.minicluster;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.IllegalConfigurationException;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.Constants;
import eu.stratosphere.test.util.filesystem.LocalFSProvider;


public class LocalClusterProvider extends ClusterProvider {

	// config parameters
	private int numTaskTrackers;

	private NepheleMiniCluster nephele;

	public LocalClusterProvider(Configuration config) throws Exception {
		super(config);

		this.numTaskTrackers = config.getInteger(Constants.CLUSTER_NUM_TASKTRACKER, -1);
		if (numTaskTrackers == -1) {
			throw new Exception("Number of task trackers was not specified");
		}
	}

	@Override
	protected void startFS() throws Exception {
		if (fsIsRunning()) {
			return;
		}

		if(config.getString(Constants.FILESYSTEM_TYPE, "").equals("local_fs")) {
			filesystemProvider = new LocalFSProvider();
		} else {
			throw new IllegalConfigurationException("Invalid file system type: "+config.getString(Constants.FILESYSTEM_TYPE, ""));
		}
		
		filesystemProvider.start();
		filesystemRunning = true;
	}

	@Override
	protected void startNephele() throws Exception {
		if (nepheleIsRunning()) {
			return;
		}

		if (filesystemProvider == null) {
			startFS();
		}

		this.nephele = new NepheleMiniCluster();
		this.nephele.setDefaultOverwriteFiles(true);
		this.nephele.start();
		this.nepheleRunning = true;
	}

	@Override
	protected void stopFS() throws Exception {
		if (!fsIsRunning()) {
			return;
		}

		filesystemProvider.stop();
		filesystemRunning = false;
	}

	@Override
	protected void stopNephele() throws Exception {
		if (!nepheleIsRunning()) {
			return;
		}
		this.nephele.stop();
		this.nepheleRunning = false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.minicluster.ClusterProvider#getJobClient(eu.stratosphere.nephele.jobgraph.JobGraph, java.lang.String)
	 */
	@Override
	public JobClient getJobClient(JobGraph jobGraph, String jarFilePath) throws Exception {
		return this.nephele.getJobClient(jobGraph);
	}
}
