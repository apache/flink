/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

public class YarnConfManager {
	private static TemporaryFolder tmp = new TemporaryFolder();


	public Map getDefaultFlinkConfig() {
		Map flink_conf = new HashMap();
		flink_conf.put("jobmanager.rpc.address", "localhost");
		flink_conf.put("jobmanager.rpc.port", 6123);
		flink_conf.put("jobmanager.heap.mb", 256);
		flink_conf.put("taskmanager.heap.mb", 512);
		flink_conf.put("taskmanager.numberOfTaskSlots", -1);
		flink_conf.put("parallelization.degree.default", 1);
		flink_conf.put("jobmanager.web.port", 8081);
		flink_conf.put("webclient.port", 8080);

		return flink_conf;
	}

	public Configuration getMiniClusterConf() {
		Configuration conf = new YarnConfiguration();
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 2 * 1024);
		conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
		conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
		conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
		conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
		conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
		conf.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
		conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
		return conf;
	}

	public File createYarnSiteConfig(Configuration yarn_conf) throws IOException {
		tmp.create();
		File yarnSiteXML = new File(tmp.newFolder().getAbsolutePath() + "/yarn-site.xml");

		FileWriter writer = new FileWriter(yarnSiteXML);
		yarn_conf.writeXml(writer);
		writer.flush();
		writer.close();
		return yarnSiteXML;
	}

	@SuppressWarnings("rawtypes")
	public File createConfigFile(Map flink_conf) throws IOException {
		tmp.create();
		File flinkConfFile = new File(tmp.newFolder().getAbsolutePath() + "/flink-conf.yaml");

		Yaml yaml = new Yaml();
		FileWriter writer = new FileWriter(flinkConfFile);

		deleteNulls(flink_conf);
		yaml.dump(flink_conf, writer);
		writer.flush();
		writer.close();
		return flinkConfFile;
	}

	@SuppressWarnings("rawtypes")
	static void deleteNulls(Map map) {
		Set set = map.entrySet();
		Iterator it = set.iterator();
		while (it.hasNext()) {
			Map.Entry m =(Map.Entry)it.next();
			if (m.getValue() == null)
				it.remove();
		}
	}
}