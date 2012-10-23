/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

/**
 * @author Erik Nijkamp
 */
public interface Constants
{
	String CLUSTER_CONFIGS = "./src/test/resources/ClusterConfigs/";

	String TEST_CONFIGS = "./src/test/resources/TestConfigs/";

	String CLUSTER_PROVIDER_TYPE = "ClusterProvider#clusterProviderType";

	String CLUSTER_PROVIDER_ID = "ClusterProvider#clusterId";
	
	String FILESYSTEM_TYPE = "FilesystemProvider#filesystemType";
	
	String CLUSTER_NUM_TASKTRACKER = "LocalClusterProvider#numTaskTrackers";

	String DEFAULT_TEST_CONFIG = "local1TM";

}
