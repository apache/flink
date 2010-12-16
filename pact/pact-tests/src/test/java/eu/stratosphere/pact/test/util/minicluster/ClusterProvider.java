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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;

public abstract class ClusterProvider {

	protected Configuration config;

	protected HDFSProvider hdfsProvider;

	protected boolean nepheleRunning = false;

	protected boolean hdfsRunning = false;

	protected static String clusterProviderType;

	public void stopCluster() throws Exception {
		stopNephele();
		stopHDFS();
	}

	public void clearDFS() throws Exception {
		clearHDFS();
	}

	public ClusterProvider(Configuration config) {
		this.config = config;
	}

	public HDFSProvider getHDFSProvider() {
		return hdfsProvider;
	}

	protected boolean nepheleIsRunning() {
		return nepheleRunning;
	}

	protected boolean hdfsIsRunning() {
		return hdfsRunning;
	}

	public abstract void submitJobAndWait(JobGraph jobGraph, String jarFilePath) throws Exception;

	protected abstract void startNephele() throws Exception;

	protected abstract void stopNephele() throws Exception;

	protected abstract void startHDFS() throws Exception;

	protected abstract void stopHDFS() throws Exception;

	protected abstract void clearHDFS() throws Exception;

}
