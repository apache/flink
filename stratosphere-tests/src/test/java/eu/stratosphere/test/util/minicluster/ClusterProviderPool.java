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

package eu.stratosphere.test.util.minicluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.Constants;

/**
 * @author Erik Nijkamp
 * @author Fabian Hueske
 */
public class ClusterProviderPool {

	protected static Hashtable<String, ClusterProvider> clusterPool = new Hashtable<String, ClusterProvider>();

	protected static Hashtable<String, Configuration> configPool = null;

	public static ClusterProvider getInstance(String clusterId) throws Exception {
		ClusterProvider instance = clusterPool.get(clusterId);
		if (instance == null) {
			// check whether config pool was initialized
			if (configPool == null) {
				loadClusterConfigs();
			}

			// get cluster config
			Configuration config = configPool.get(clusterId);
			if (config == null) {
				throw new Exception("No cluster configuration known for clusterId " + clusterId);
			}

			// bring up new cluster
			String clusterProviderType = config.getString(Constants.CLUSTER_PROVIDER_TYPE, "");
			if (clusterProviderType.equals("LocalClusterProvider")) {
				instance = new LocalClusterProvider(config);
			} else {
				throw new Exception("No or unknown cluster provider type configured");
			}
			instance.startFS();
			instance.startNephele();

			clusterPool.put(clusterId, instance);
		}
		return instance;
	}

	public static void removeInstance(String clusterId) throws Exception {
		clusterPool.remove(clusterId);
	}

	private static void loadClusterConfigs() throws FileNotFoundException, IOException {
		configPool = new Hashtable<String, Configuration>();

		File configDir = new File(Constants.CLUSTER_CONFIGS);

		if (configDir.isDirectory()) {
			for (File configFile : configDir.listFiles()) {
				// try to parse config file
				Configuration clusterConfig = new Configuration();
				Properties p = new Properties();
				p.load(new FileInputStream(configFile));

				for (String key : p.stringPropertyNames()) {
					clusterConfig.setString(key, p.getProperty(key));
				}

				// add config to config pool
				String clusterId = clusterConfig.getString(Constants.CLUSTER_PROVIDER_ID, "");
				if (clusterId.equals("")) {
					throw new IllegalArgumentException("Cluster Configuration has no clusterId");
				}
				configPool.put(clusterId, clusterConfig);
			}
		}
	}

}
